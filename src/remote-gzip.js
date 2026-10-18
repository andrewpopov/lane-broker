import zlib from 'node:zlib';
import { Transform, Writable, pipeline } from 'node:stream';

// BRAIN-642: a runner that lists this accepts `lane remote-exec --transfer gzip`; a client sends it only to such a runner.
export const SNAPSHOT_GZIP_CAPABILITY = 'snapshot-gzip/1';
export const TRANSFER_GZIP = 'gzip';

/** BRAIN-642: `lane remote-exec` exits with this, after a one-line reason on stderr, when it refused the stream before creating any
 *  ticket. It is the runner's positive statement that nothing was started; the client trusts exactly this code and no other. */
export const REMOTE_EXEC_NO_TICKET_EXIT = 66;

const GZIP_HEADER_BYTES = 10;
const GZIP_TRAILER_BYTES = 8;

const CRC_TABLE = new Uint32Array(256).map((_, n) => {
  let c = n;
  for (let k = 0; k < 8; k++) c = c & 1 ? 0xedb88320 ^ (c >>> 1) : c >>> 1;
  return c >>> 0;
});

/** Running CRC-32 (gzip's); engines allows Node 20, where `zlib.crc32` may be missing, so it is computed here. */
function crc32Update(crc, chunk) {
  let c = ~crc;
  for (let i = 0; i < chunk.length; i++) c = CRC_TABLE[(c ^ chunk[i]) & 0xff] ^ (c >>> 8);
  return ~c >>> 0;
}

/** Parse the `--transfer` value off `lane remote-exec`'s args: `{ok, transfer}` (undefined = the legacy plain stream). */
export function parseTransferFlag(args) {
  const idx = args.indexOf('--transfer');
  if (idx === -1) return { ok: true, transfer: undefined };
  const value = args[idx + 1];
  if (value !== TRANSFER_GZIP) return { ok: false, reason: `unknown --transfer value: ${value === undefined ? '(missing)' : value}` };
  return { ok: true, transfer: value };
}

/** Pass-through that reports each chunk's size; the client's raw and sent byte counters. */
export function countBytes(onBytes) {
  return new Transform({
    transform(chunk, _enc, cb) {
      onBytes(chunk.length);
      cb(null, chunk);
    },
  });
}

/**
 * The runner's decompressor for a `--transfer gzip` stream. Node's Gunzip would accept concatenated members (an appended empty
 * member costs work and yields no bytes), so the gzip framing is parsed here: exactly one fixed 10-byte header, one raw deflate
 * stream, one 8-byte trailer, nothing after. Returns `{ input, output }`: write the compressed bytes to `input`; read the decoded
 * bytes from `output`. `output` ends only after the whole 8-byte trailer has arrived and its CRC-32 and size match the decoded
 * bytes, so a consumer that sees the end of `output` has seen a complete, verified member. Past `maxDecodedBytes` decoded or `maxCompressedBytes` compressed, or on any framing/corruption error,
 * every stage is destroyed and `output` errors, so the reader sees a failure rather than a hang.
 */
export function createStrictGunzip({ maxDecodedBytes, maxCompressedBytes }) {
  const inflate = zlib.createInflateRaw();
  let decoded = 0;
  let crc = 0;
  let trailerArrived;
  const trailerReceived = new Promise((resolve) => {
    trailerArrived = resolve;
  });
  const counter = new Transform({
    transform(chunk, _enc, cb) {
      decoded += chunk.length;
      crc = crc32Update(crc, chunk);
      if (decoded > maxDecodedBytes) return cb(new Error(`decompressed stream exceeds ${maxDecodedBytes} bytes`));
      return cb(null, chunk);
    },
    // The deflate stream has ended, but the member has not: hold the end of the output until the input has delivered a valid trailer.
    flush(cb) {
      trailerReceived.then((t) => {
        if (t.readUInt32LE(0) !== crc) return cb(new Error('gzip trailer checksum mismatch'));
        if (t.readUInt32LE(4) !== decoded >>> 0) return cb(new Error('gzip trailer size mismatch'));
        return cb();
      });
    },
  });
  pipeline(inflate, counter, () => {});

  let compressed = 0;
  let head = Buffer.alloc(0);
  let headerDone = false;
  let trailer = Buffer.alloc(0);
  const fail = (err) => {
    inflate.destroy();
    counter.destroy(err);
  };

  const input = new Writable({
    write(chunk, _enc, cb) {
      compressed += chunk.length;
      if (compressed > maxCompressedBytes) return cb(new Error(`compressed stream exceeds ${maxCompressedBytes} bytes`));
      let body = chunk;
      if (!headerDone) {
        head = Buffer.concat([head, chunk]);
        if (head.length < GZIP_HEADER_BYTES) return cb();
        if (head[0] !== 0x1f || head[1] !== 0x8b || head[2] !== 0x08 || head[3] !== 0) return cb(new Error('not a plain gzip stream'));
        body = head.subarray(GZIP_HEADER_BYTES);
        headerDone = true;
        head = null;
      }
      if (body.length === 0) return cb();
      const before = inflate.bytesWritten;
      return inflate.write(body, (err) => {
        if (err) return cb(err);
        // Once the deflate stream ends, the engine consumes nothing more: the rest of the input is the trailer or trailing junk.
        const consumed = inflate.bytesWritten - before;
        if (consumed < body.length) trailer = Buffer.concat([trailer, body.subarray(consumed)]);
        if (trailer.length > GZIP_TRAILER_BYTES) return cb(new Error('compressed data after the end of the gzip member'));
        return cb();
      });
    },
    final(cb) {
      if (!headerDone) return cb(new Error('truncated gzip stream: missing header'));
      // A deflate stream cut short surfaces as the inflate 'error' (unexpected end of file) from this end().
      inflate.end(() => {
        if (trailer.length !== GZIP_TRAILER_BYTES) return cb(new Error('truncated gzip stream: missing trailer'));
        trailerArrived(trailer);
        return cb();
      });
    },
  });

  inflate.on('error', fail);
  input.on('error', fail);
  counter.on('close', () => {
    if (!input.destroyed) input.destroy();
  });
  return { input, output: counter };
}
