const REDIRECT = new URL('./queue-redirect-db.js', import.meta.url).href;

export async function resolve(specifier, context, nextResolve) {
  const resolved = await nextResolve(specifier, context);
  return resolved.url.endsWith('/src/lane-db.js') ? { ...resolved, url: REDIRECT } : resolved;
}
