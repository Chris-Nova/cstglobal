// lib/auth.ts
// Temporary stub — returns a mock user so API routes work before
// real NextAuth session checking is implemented.
// Replace the body of requireAuth with real session logic when ready.

import { NextRequest } from "next/server";

export interface AuthUser {
  id:    string;
  email: string;
  plan:  "Free" | "Pro" | "Enterprise";
}

export async function requireAuth(req: NextRequest): Promise<AuthUser> {
  // TODO: replace with real session check e.g.
  // const session = await getServerSession(authOptions);
  // if (!session) throw Object.assign(new Error("Unauthorized"), { code: "UNAUTHORIZED" });
  // return session.user as AuthUser;

  // Stub: always returns a Pro user for local development
  return {
    id:    "00000000-0000-0000-0000-000000000001",
    email: "dev@cstglobal.local",
    plan:  "Pro",
  };
}

export function requirePlan(user: AuthUser, required: "Pro" | "Enterprise") {
  const rank = { Free: 0, Pro: 1, Enterprise: 2 };
  if (rank[user.plan] < rank[required]) {
    throw Object.assign(
      new Error(`${required} plan required`),
      { code: "PLAN_REQUIRED" }
    );
  }
}
