// app/api/leads/[id]/route.ts
import { NextRequest, NextResponse } from 'next/server';
import { query } from '../../../lib/db';
import { requireAuth } from '../../../lib/auth';
import { z } from 'zod';

export async function PUT(
  req: NextRequest,
  context: { params: Promise<{ id: string }> }
) {
  try {
    const params = await context.params;
    const id = params.id;
    const user = await requireAuth(req);

    const Schema = z.object({
      status:          z.enum(['Discovery','Qualifying','Bidding','Won','Lost']).optional(),
      notes:           z.string().max(5000).optional(),
      bid_value_usd:   z.number().positive().optional(),
      probability_pct: z.number().min(0).max(100).optional(),
      position:        z.number().int().min(0).optional(),
      tags:            z.array(z.string()).optional(),
    });

    const body = Schema.parse(await req.json());
    const sets: string[] = ['updated_at = NOW()'];
    const values: unknown[] = [id, user.id];
    let i = 3;

    if (body.status !== undefined)          { sets.push(`status = $${i++}::lead_status`); values.push(body.status); }
    if (body.notes !== undefined)           { sets.push(`notes = $${i++}`);               values.push(body.notes); }
    if (body.bid_value_usd !== undefined)   { sets.push(`bid_value = $${i++}`);           values.push(body.bid_value_usd * 100); }
    if (body.probability_pct !== undefined) { sets.push(`probability_pct = $${i++}`);     values.push(body.probability_pct); }
    if (body.position !== undefined)        { sets.push(`position = $${i++}`);            values.push(body.position); }
    if (body.tags !== undefined)            { sets.push(`tags = $${i++}`);                values.push(body.tags); }

    const result = await query(
      `UPDATE leads SET ${sets.join(', ')} WHERE id = $1 AND user_id = $2 RETURNING id, status, updated_at`,
      values
    );

    if (result.rowCount === 0) {
      return NextResponse.json({ error: 'Lead not found' }, { status: 404 });
    }
    return NextResponse.json({ data: result.rows[0] });
  } catch (err: any) {
    if (err.name === 'ZodError') return NextResponse.json({ error: 'Invalid body' }, { status: 400 });
    console.error('[PUT /api/leads/[id]]', err);
    return NextResponse.json({ error: 'Internal server error' }, { status: 500 });
  }
}

export async function DELETE(
  req: NextRequest,
  context: { params: Promise<{ id: string }> }
) {
  try {
    const params = await context.params;
    const id = params.id;
    const user = await requireAuth(req);

    const result = await query(
      'DELETE FROM leads WHERE id = $1 AND user_id = $2 RETURNING id',
      [id, user.id]
    );

    if (result.rowCount === 0) {
      return NextResponse.json({ error: 'Lead not found' }, { status: 404 });
    }
    return NextResponse.json({ data: { deleted: id } });
  } catch (err) {
    console.error('[DELETE /api/leads/[id]]', err);
    return NextResponse.json({ error: 'Internal server error' }, { status: 500 });
  }
}