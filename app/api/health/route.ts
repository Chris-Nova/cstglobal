// app/api/health/route.ts
// Used by Docker healthcheck: curl -f http://localhost:3000/api/health

import { NextResponse } from 'next/server';
import { query } from '@/lib/db';

export async function GET() {
  try {
    // Ping the database
    const { rows } = await query('SELECT NOW() AS db_time, version() AS db_version');

    return NextResponse.json({
      status:   'ok',
      db:       'connected',
      db_time:  rows[0].db_time,
      app_time: new Date().toISOString(),
    });
  } catch (err: any) {
    return NextResponse.json(
      { status: 'error', db: 'disconnected', error: err.message },
      { status: 503 }
    );
  }
}
