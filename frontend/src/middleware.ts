import { Auth0Client } from '@auth0/nextjs-auth0/server';
import { NextRequest, NextResponse } from 'next/server';

const auth0 = new Auth0Client();

export async function middleware(request: NextRequest) {
    // Let auth routes through to the SDK
    const authResponse = await auth0.middleware(request);
    if (request.nextUrl.pathname.startsWith('/auth/')) {
        return authResponse;
    }

    // For all other routes, require authentication
    const session = await auth0.getSession(request);
    if (!session) {
        return NextResponse.redirect(
            new URL('/auth/login?returnTo=' + encodeURIComponent(request.nextUrl.pathname), request.url)
        );
    }

    return authResponse;
}

export const config = {
    matcher: ['/((?!_next/static|_next/image|favicon.ico|icon.svg|bhg-logo.png).*)'],
};
