// app/api/leads/route.ts
// GET  /api/leads        — fetch current user's kanban board
// POST /api/leads        — save a project as a lead
// PUT  /api/leads/[id]   — move stage, update notes, set bid value
// DELETE /api/leads/[id] — remove lead
 
import { NextRequest, NextResponse } from 'next/server';
import { query, withTransaction } from '@/lib/db';
import { requireAuth } from '@/lib/auth';
import { z } from 'zod';
 
// ── GET /api/leads ────────────────────────────────────────────
// Returns leads grouped by kanban stage for the authenticated user
export async function GET(req: NextRequest) {
  const user = await requireAuth(req);
 
  const result = await query(`
    SELECT
      l.id              AS lead_id,
      l.status,
      l.notes,
      l.bid_value / 100 AS bid_value_usd,
      l.probability_pct,
      l.tags,
      l.position,
      l.created_at      AS tracked_since,
      l.updated_at,
      p.id              AS project_id,
      p.title,
      p.value_usd / 100 AS value_usd,
      p.location_display,
      p.region,
      p.sector,
      p.stage           AS project_stage,
      p.timeline_display,
      p.last_verified_at,
      COALESCE(ps.total_score, 0) AS score,
      -- Upcoming reminders
      COALESCE(
        JSON_AGG(
          JSON_BUILD_OBJECT(
            'id',         r.id,
            'title',      r.title,
            'remind_at',  r.remind_at,
            'is_sent',    r.is_sent
          )
        ) FILTER (WHERE r.id IS NOT NULL AND r.is_sent = FALSE),
        '[]'
      ) AS reminders
    FROM leads l
    JOIN projects p  ON p.id = l.project_id
    LEFT JOIN project_scores ps
      ON ps.project_id = p.id
      AND (ps.user_id = $1 OR ps.user_id IS NULL)
    LEFT JOIN lead_reminders r ON r.lead_id = l.id
    WHERE l.user_id = $1
    GROUP BY l.id, p.id, ps.total_score
    ORDER BY l.status, l.position ASC
  `, [user.id]);
 
  // Group into kanban columns
  const board: Record<string, typeof result.rows> = {
    Discovery: [], Qualifying: [], Bidding: [], Won: [], Lost: [],
  };
  for (const row of result.rows) {
    if (board[row.status]) board[row.status].push(row);
  }
 
  return NextResponse.json({ data: board });
}
 
// ── POST /api/leads ───────────────────────────────────────────
export async function POST(req: NextRequest) {
  const user = await requireAuth(req);
 
  const Schema = z.object({
    project_id: z.string().uuid(),
    status:     z.enum(['Discovery','Qualifying','Bidding','Won','Lost']).default('Discovery'),
    notes:      z.string().max(5000).optional(),
  });
 
  const body = Schema.parse(await req.json());
 
  // Check plan limits (Free = 5 leads max)
  if (user.plan === 'Free') {
    const { rows } = await query(
      'SELECT COUNT(*) AS c FROM leads WHERE user_id = $1', [user.id]
    );
    if (parseInt(rows[0].c) >= 5) {
      return NextResponse.json(
        { error: 'Free plan limited to 5 tracked leads. Upgrade to Pro.' },
        { status: 403 }
      );
    }
  }
 
  const result = await query(`
    INSERT INTO leads (user_id, project_id, status, notes)
    VALUES ($1, $2, $3::lead_status, $4)
    ON CONFLICT (user_id, project_id)
      DO UPDATE SET status = EXCLUDED.status, notes = EXCLUDED.notes, updated_at = NOW()
    RETURNING id, status, created_at
  `, [user.id, body.project_id, body.status, body.notes]);
 
  return NextResponse.json({ data: result.rows[0] }, { status: 201 });
}