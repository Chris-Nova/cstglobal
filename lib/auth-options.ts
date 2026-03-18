// lib/auth-options.ts
// Central NextAuth configuration.
// Supports: email/password credentials + Google OAuth + Microsoft OAuth
//
// Session strategy: JWT (stateless) — no DB session table needed for basic auth.
// The JWT contains id, email, plan so API routes can gate on plan without a DB hit.

import type { NextAuthOptions } from "next-auth";
import CredentialsProvider from "next-auth/providers/credentials";
import GoogleProvider from "next-auth/providers/google";
import AzureADProvider from "next-auth/providers/azure-ad";
import { query } from "@/lib/db";
import bcrypt from "bcryptjs";
import { z } from "zod";

// ── Types ─────────────────────────────────────────────────────
// Extend NextAuth's default types so plan is available everywhere
declare module "next-auth" {
  interface Session {
    user: {
      id:    string;
      email: string;
      name:  string | null;
      plan:  "Free" | "Pro" | "Enterprise";
    };
  }
  interface User {
    id:    string;
    plan:  "Free" | "Pro" | "Enterprise";
  }
}

declare module "next-auth/jwt" {
  interface JWT {
    id:   string;
    plan: "Free" | "Pro" | "Enterprise";
  }
}

// ── Credentials login schema ──────────────────────────────────
const CredentialsSchema = z.object({
  email:    z.string().email(),
  password: z.string().min(8),
});

// ── Main options object ───────────────────────────────────────
export const authOptions: NextAuthOptions = {
  // Use JWT sessions (no adapter needed, no extra DB table)
  session: {
    strategy: "jwt",
    maxAge:   30 * 24 * 60 * 60,   // 30 days
  },

  pages: {
    signIn:  "/auth/login",
    signOut: "/auth/login",
    error:   "/auth/login",         // errors passed as ?error= query param
    newUser: "/auth/register",
  },

  providers: [
    // ── Email / Password ──────────────────────────────────────
    CredentialsProvider({
      name: "Email",
      credentials: {
        email:    { label: "Email",    type: "email"    },
        password: { label: "Password", type: "password" },
      },
      async authorize(credentials) {
        // 1. Validate input shape
        const parsed = CredentialsSchema.safeParse(credentials);
        if (!parsed.success) {
          throw new Error("Invalid email or password format");
        }

        const { email, password } = parsed.data;

        // 2. Look up user
        const { rows } = await query(
          "SELECT id, email, full_name, hashed_password, plan, is_active, is_verified FROM users WHERE email = $1",
          [email.toLowerCase()]
        );

        const user = rows[0];
        if (!user) throw new Error("No account found with that email");
        if (!user.is_active) throw new Error("Account is deactivated");
        if (!user.hashed_password) throw new Error("Please sign in with Google or Microsoft");

        // 3. Verify password
        const valid = await bcrypt.compare(password, user.hashed_password);
        if (!valid) throw new Error("Incorrect password");

        // 4. Update last login
        await query("UPDATE users SET last_login_at = NOW() WHERE id = $1", [user.id]);

        return {
          id:    user.id,
          email: user.email,
          name:  user.full_name,
          plan:  user.plan,
        };
      },
    }),

    // ── Google OAuth ──────────────────────────────────────────
    GoogleProvider({
      clientId:     process.env.GOOGLE_CLIENT_ID!,
      clientSecret: process.env.GOOGLE_CLIENT_SECRET!,
      authorization: {
        params: { prompt: "consent", access_type: "offline" },
      },
    }),

    // ── Microsoft / Azure AD ──────────────────────────────────
    AzureADProvider({
      clientId:     process.env.AZURE_AD_CLIENT_ID!,
      clientSecret: process.env.AZURE_AD_CLIENT_SECRET!,
      tenantId:     process.env.AZURE_AD_TENANT_ID || "common",
    }),
  ],

  callbacks: {
    // ── On OAuth sign-in: upsert user into DB ─────────────────
    async signIn({ user, account }) {
      if (account?.type === "oauth") {
        try {
          // Check if user already exists
          const { rows } = await query(
            "SELECT id, plan FROM users WHERE email = $1",
            [user.email!.toLowerCase()]
          );

          if (rows.length === 0) {
            // New OAuth user — create account with Free plan
            const { rows: newRows } = await query(`
              INSERT INTO users (email, full_name, is_active, is_verified, plan)
              VALUES ($1, $2, TRUE, TRUE, 'Free')
              RETURNING id, plan
            `, [user.email!.toLowerCase(), user.name]);

            user.id   = newRows[0].id;
            user.plan = newRows[0].plan;
          } else {
            user.id   = rows[0].id;
            user.plan = rows[0].plan;
            await query("UPDATE users SET last_login_at = NOW() WHERE id = $1", [rows[0].id]);
          }

          // Upsert OAuth account record
          await query(`
            INSERT INTO oauth_accounts (user_id, provider, provider_id, access_token, refresh_token, expires_at)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (provider, provider_id)
              DO UPDATE SET access_token = EXCLUDED.access_token, refresh_token = EXCLUDED.refresh_token
          `, [
            user.id,
            account.provider,
            account.providerAccountId,
            account.access_token,
            account.refresh_token,
            account.expires_at ? new Date(account.expires_at * 1000) : null,
          ]);

        } catch (err) {
          console.error("[NextAuth signIn callback]", err);
          return false;   // block sign-in on DB error
        }
      }
      return true;
    },

    // ── Encode extra fields into JWT ──────────────────────────
    async jwt({ token, user }) {
      if (user) {
        // First sign-in: user object is populated
        token.id   = user.id;
        token.plan = user.plan;
      }
      return token;
    },

    // ── Expose JWT fields on the session object ───────────────
    async session({ session, token }) {
      if (token) {
        session.user.id   = token.id;
        session.user.plan = token.plan;
      }
      return session;
    },
  },

  // Encrypt the JWT cookie
  secret: process.env.NEXTAUTH_SECRET,

  debug: process.env.NODE_ENV === "development",
};
