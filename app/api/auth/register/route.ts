// app/api/auth/register/route.ts
// POST /api/auth/register
// Creates a new user account with hashed password.
// After registration the client should call signIn() from next-auth/react.

import { NextRequest, NextResponse } from "next/server";
import { query } from "@/lib/db";
import bcrypt from "bcryptjs";
import { z } from "zod";

const RegisterSchema = z.object({
  email:    z.string().email("Invalid email address"),
  password: z.string()
    .min(8,  "Password must be at least 8 characters")
    .regex(/[A-Z]/, "Password must contain at least one uppercase letter")
    .regex(/[0-9]/, "Password must contain at least one number"),
  fullName: z.string().min(2, "Name must be at least 2 characters").max(255),
  company:  z.string().max(255).optional(),
});

export async function POST(req: NextRequest) {
  try {
    const body = await req.json();
    const data = RegisterSchema.parse(body);

    // Check if email already exists
    const { rows: existing } = await query(
      "SELECT id FROM users WHERE email = $1",
      [data.email.toLowerCase()]
    );

    if (existing.length > 0) {
      return NextResponse.json(
        { error: "An account with this email already exists" },
        { status: 409 }
      );
    }

    // Hash password with bcrypt (12 rounds — good balance of security/speed)
    const hashedPassword = await bcrypt.hash(data.password, 12);

    // Create user (Free plan by default)
    const { rows } = await query(`
      INSERT INTO users (email, hashed_password, full_name, company, plan, is_active, is_verified)
      VALUES ($1, $2, $3, $4, 'Free', TRUE, FALSE)
      RETURNING id, email, full_name, plan, created_at
    `, [
      data.email.toLowerCase(),
      hashedPassword,
      data.fullName,
      data.company || null,
    ]);

    const user = rows[0];

    // TODO: send verification email here
    // await sendVerificationEmail(user.email, user.id);

    return NextResponse.json({
      message: "Account created successfully",
      user: {
        id:    user.id,
        email: user.email,
        name:  user.full_name,
        plan:  user.plan,
      },
    }, { status: 201 });

  } catch (err: any) {
    if (err.name === "ZodError") {
      // Return the first validation error message
      return NextResponse.json(
        { error: err.issues[0].message },
        { status: 400 }
      );
    }
    console.error("[POST /api/auth/register]", err);
    return NextResponse.json(
      { error: "Registration failed. Please try again." },
      { status: 500 }
    );
  }
}
