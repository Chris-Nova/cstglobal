// app/api/scores/route.ts
// POST /api/scores/calculate — run the scoring formula for a user+project pair
// GET  /api/scores?project_id= — retrieve stored score

import { NextRequest, NextResponse } from 'next/server';
import { query } from '../../../lib/db';

const DEMO_USER = { id: '00000000-0000-0000-0000-000000000001', email: 'demo@cstglobal.app', plan: 'Pro' as const };
async function getUser() { return DEMO_USER; }
import { z } from 'zod';

// ────────────────────────────────────────────────────────────
// AI SCORING ENGINE
// Formula:  S = (w1 · Match) + (w2 · Budget) + (w3 · Timeline)
// Weights:  w1=0.40, w2=0.35, w3=0.25  (sum = 1.0)
// Each component scored 0–100, then weighted
// Final score rounded to nearest integer 0–100
// ────────────────────────────────────────────────────────────

interface UserPreferences {
  preferred_sectors:    string[];
  preferred_regions:    string[];
  min_budget_usd:       number;
  max_budget_usd:       number;
  preferred_stages:     string[];
  timeline_horizon_months: number;
}

interface ProjectData {
  sector:          string;
  region:          string;
  value_usd:       number;
  stage:           string;
  timeline_start:  string | null;
  timeline_end:    string | null;
}

// ── Scoring sub-functions ────────────────────────────────────

function scoreMatch(project: ProjectData, prefs: UserPreferences): number {
  let score = 0;

  // Sector match: 0, 50, or 100
  if (prefs.preferred_sectors.length === 0) {
    score += 50;
  } else if (prefs.preferred_sectors.includes(project.sector)) {
    score += 100;
  } else {
    score += 0;
  }

  // Region match: 0 or 100
  if (prefs.preferred_regions.length === 0) {
    score += 50;
  } else if (prefs.preferred_regions.includes(project.region)) {
    score += 100;
  } else {
    score += 0;
  }

  // Stage match
  if (prefs.preferred_stages.length === 0) {
    score += 50;
  } else if (prefs.preferred_stages.includes(project.stage)) {
    score += 100;
  } else {
    score += 0;
  }

  return Math.round(score / 3);   // average of 3 sub-dimensions
}

function scoreBudget(project: ProjectData, prefs: UserPreferences): number {
  const v = project.value_usd;
  const min = prefs.min_budget_usd;
  const max = prefs.max_budget_usd;

  if (min === 0 && max === 0) return 50;   // no preference set

  if (v >= min && v <= max) return 100;    // perfect fit

  if (v < min) {
    // Below range — score degrades as project is smaller
    const ratio = v / min;
    return Math.round(ratio * 70);         // max 70 for below-range
  }

  // Above range — score degrades as project is larger
  const ratio = max / v;
  return Math.round(ratio * 80);           // max 80 for above-range
}

function scoreTimeline(project: ProjectData, prefs: UserPreferences): number {
  if (!project.timeline_start) return 40;  // unknown timeline = neutral

  const horizonMs = prefs.timeline_horizon_months * 30 * 24 * 60 * 60 * 1000;
  const startDate = new Date(project.timeline_start);
  const now = new Date();
  const distMs = startDate.getTime() - now.getTime();

  if (distMs < 0) return 60;               // already started = good

  const monthsAway = distMs / (30 * 24 * 60 * 60 * 1000);

  if (monthsAway <= prefs.timeline_horizon_months) {
    // Within horizon: closer = better
    const ratio = 1 - (monthsAway / prefs.timeline_horizon_months);
    return Math.round(60 + ratio * 40);    // 60–100
  }

  // Beyond horizon
  return Math.round(Math.max(0, 60 - (monthsAway - prefs.timeline_horizon_months) * 2));
}

// ── Master scoring function ──────────────────────────────────
export function calculateScore(
  project: ProjectData,
  prefs: UserPreferences
): { total: number; match: number; budget: number; timeline: number } {
  const W1 = 0.40, W2 = 0.35, W3 = 0.25;

  const match    = scoreMatch(project, prefs);
  const budget   = scoreBudget(project, prefs);
  const timeline = scoreTimeline(project, prefs);

  const total = Math.round(W1 * match + W2 * budget + W3 * timeline);

  return { total: Math.min(100, Math.max(0, total)), match, budget, timeline };
}

// ── API Route ─────────────────────────────────────────────────

export async function POST(req: NextRequest) {
  const user = await getUser();

  const Schema = z.object({
    project_id: z.string().uuid(),
    // Optional user prefs override (otherwise load from DB)
    preferences: z.object({
      preferred_sectors:       z.array(z.string()).default([]),
      preferred_regions:       z.array(z.string()).default([]),
      min_budget_usd:          z.number().default(0),
      max_budget_usd:          z.number().default(0),
      preferred_stages:        z.array(z.string()).default([]),
      timeline_horizon_months: z.number().default(24),
    }).optional(),
  });

  const body = Schema.parse(await req.json());

  // Load project
  const { rows: projects } = await query(`
    SELECT sector, region, value_usd / 100 AS value_usd, stage,
           timeline_start, timeline_end
    FROM projects WHERE id = $1 AND is_active = TRUE
  `, [body.project_id]);

  if (!projects.length) {
    return NextResponse.json({ error: 'Project not found' }, { status: 404 });
  }

  // Use provided prefs or fall back to defaults
  const prefs: UserPreferences = body.preferences ?? {
    preferred_sectors: [],
    preferred_regions: [],
    min_budget_usd: 0,
    max_budget_usd: 0,
    preferred_stages: ['Tender', 'Planning'],
    timeline_horizon_months: 24,
  };

  const scores = calculateScore(projects[0], prefs);

  // Upsert into DB
  await query(`
    INSERT INTO project_scores (project_id, user_id, total_score, match_score, budget_score, timeline_score, factors)
    VALUES ($1, $2, $3, $4, $5, $6, $7)
    ON CONFLICT (project_id, user_id)
      DO UPDATE SET
        total_score    = EXCLUDED.total_score,
        match_score    = EXCLUDED.match_score,
        budget_score   = EXCLUDED.budget_score,
        timeline_score = EXCLUDED.timeline_score,
        factors        = EXCLUDED.factors,
        calculated_at  = NOW()
  `, [
    body.project_id, user.id,
    scores.total, scores.match, scores.budget, scores.timeline,
    JSON.stringify({ prefs_used: prefs, version: '1.0' }),
  ]);

  return NextResponse.json({
    data: {
      project_id:     body.project_id,
      total_score:    scores.total,
      match_score:    scores.match,
      budget_score:   scores.budget,
      timeline_score: scores.timeline,
      weights:        { match: 0.40, budget: 0.35, timeline: 0.25 },
    },
  });
}