import { NextResponse } from "next/server";

// admin.unitleague.com (and admin.localhost in dev) serves the /admin routes.
// unitleague.com serves everything else and hides /admin.
export function proxy(request) {
  const host = request.headers.get("host") ?? "";
  const url = request.nextUrl.clone();
  const isAdminHost = host.startsWith("admin.");
  const isAdminPath = url.pathname === "/admin" || url.pathname.startsWith("/admin/");

  if (isAdminHost && !isAdminPath) {
    url.pathname = `/admin${url.pathname === "/" ? "" : url.pathname}`;
    return NextResponse.rewrite(url);
  }
  if (!isAdminHost && isAdminPath) {
    url.pathname = "/_not-found";
    return NextResponse.rewrite(url);
  }
  return NextResponse.next();
}

export const config = {
  matcher: ["/((?!_next/|favicon.ico|logo-).*)"],
};
