// app/api/projects/route.ts
import { NextRequest, NextResponse } from 'next/server';
import { query } from '../../../lib/db';
import { z } from 'zod';

const GetProjectsSchema = z.object({
  region:  z.string().optional(),
  sector:  z.string().optional(),
  stage:   z.string().optional(),
  source:  z.string().optional(),
  q:       z.string().max(200).optional(),
  page:    z.coerce.number().min(1).default(1),
  limit:   z.coerce.number().min(1).max(100).default(24),
  sortBy:  z.enum(['value_usd', 'last_verified_at', 'created_at']).default('last_verified_at'),
  sortDir: z.enum(['asc', 'desc']).default('desc'),
});

export async function GET(req: NextRequest) {
  try {
    const params = GetProjectsSchema.parse(
      Object.fromEntries(req.nextUrl.searchParams)
    );

    const limit  = params.limit;
    const offset = (params.page - 1) * limit;

    const conditions: string[] = [];
    const values: unknown[]    = [];
    let i = 1;

    if (params.region) {
      conditions.push(`p.region = $${i++}::project_region`);
      values.push(params.region);
    }
    if (params.sector) {
      conditions.push(`p.sector = $${i++}::project_sector`);
      values.push(params.sector);
    }
    if (params.stage) {
      conditions.push(`p.stage = $${i++}::project_stage`);
      values.push(params.stage);
    }
    if (params.source) {
      conditions.push(`p.source_name = $${i++}`);
      values.push(params.source);
    }
    if (params.q) {
      conditions.push(`p.title ILIKE $${i++}`);
      values.push(`%${params.q}%`);
    }

    const where = conditions.length > 0 ? 'WHERE ' + conditions.join(' AND ') : '';

    const sql = `
      SELECT
        p.id,
        p.title,
        p.description,
        p.value_usd,
        p.value_currency,
        p.location_display,
        p.location_country,
        p.region,
        p.sector,
        p.stage,
        p.timeline_display,
        p.milestones,
        p.active_milestone,
        p.source_name,
        p.source_url,
        p.tender_document_url,
        p.last_verified_at,
        p.first_seen_at,
        ST_AsGeoJSON(p.geog)::json AS geojson,
        COALESCE(ps.total_score, 50) AS score,
        COALESCE(
          JSON_AGG(
            JSON_BUILD_OBJECT(
              'role', pst.role,
              'name', COALESCE(o.name, pst.name_override)
            )
          ) FILTER (WHERE pst.id IS NOT NULL),
          '[]'
        ) AS stakeholders
      FROM projects p
      LEFT JOIN project_scores ps ON ps.project_id = p.id
      LEFT JOIN project_stakeholders pst ON pst.project_id = p.id
      LEFT JOIN organisations o ON o.id = pst.organisation_id
      ${where}
      GROUP BY p.id, ps.total_score
      ORDER BY p.${params.sortBy} ${params.sortDir}
      LIMIT $${i} OFFSET $${i + 1}
    `;
    values.push(limit, offset);

    const countSql = `SELECT COUNT(*) AS total FROM projects p ${where}`;
    const countValues = values.slice(0, i - 1);

    const [dataResult, countResult] = await Promise.all([
      query(sql, values),
      query(countSql, countValues),
    ]);

    const total = parseInt(countResult.rows[0].total, 10);

    return NextResponse.json({
      data: dataResult.rows,
      meta: { total, page: params.page, limit, pages: Math.ceil(total / limit) },
    });

  } catch (err: any) {
    if (err.name === 'ZodError') {
      return NextResponse.json({ error: 'Invalid query params' }, { status: 400 });
    }
    console.error('[GET /api/projects]', err);
    return NextResponse.json({ error: 'Internal server error' }, { status: 500 });
  }
}
