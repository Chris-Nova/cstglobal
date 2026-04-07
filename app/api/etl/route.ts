// app/api/etl/route.ts
// Trigger ETL runs and get source status from the UI
import { NextRequest, NextResponse } from 'next/server';
import { query } from '../../../lib/db';

const VALID_SOURCES = [
  'ted_eu','sam_gov','contracts_finder','world_bank',
  'uk_planning','nsw_eplanning',
  'nyc_permits','chicago_permits','la_permits','houston_permits','philly_permits','usace',
  'philly_arcgis','denver_permits','sf_permits','boston_permits','sj_permits','baltimore_permits','nashville_permits','nola_permits','lv_permits','raleigh_permits',
];

const SOURCE_META: Record<string, { label: string; region: string; type: string }> = {
  ted_eu:           { label: "TED EU",              region: "Europe",       type: "Public Tender" },
  sam_gov:          { label: "SAM.gov",             region: "Americas",     type: "Public Tender" },
  contracts_finder: { label: "Contracts Finder",    region: "Europe",       type: "Public Tender" },
  world_bank:       { label: "World Bank",          region: "Global",       type: "Development Project" },
  uk_planning:      { label: "UK Planning Portal",  region: "Europe",       type: "Planning Application" },
  nsw_eplanning:    { label: "NSW ePlanning",       region: "Asia Pacific", type: "Planning Application" },
  nyc_permits:      { label: "NYC DOB",             region: "Americas",     type: "City Permit" },
  chicago_permits:  { label: "Chicago Permits",     region: "Americas",     type: "City Permit" },
  la_permits:       { label: "LA Building & Safety",region: "Americas",     type: "City Permit" },
  houston_permits:  { label: "Houston Permits",     region: "Americas",     type: "City Permit" },
  philly_permits:   { label: "Seattle Permits",     region: "Americas",     type: "City Permit" },
  usace:            { label: "USACE",               region: "Americas",     type: "Federal Infrastructure" },
  philly_arcgis:    { label: "Philadelphia L&I",   region: "Americas",     type: "City Permit" },
  denver_permits:   { label: "Dallas Permits",     region: "Americas",     type: "City Permit" },
  sf_permits:       { label: "SF Building Permits",region: "Americas",     type: "City Permit" },
  boston_permits:   { label: "Boston Permits",      region: "Americas",     type: "City Permit" },
  sj_permits:       { label: "San Jose Permits",    region: "Americas",     type: "City Permit" },
  baltimore_permits:{ label: "Baltimore Permits",   region: "Americas",     type: "City Permit" },
  nashville_permits:{ label: "Nashville Permits",  region: "Americas",     type: "City Permit" },
  nola_permits:     { label: "New Orleans Permits",region: "Americas",     type: "City Permit" },
  lv_permits:       { label: "Las Vegas Permits",  region: "Americas",     type: "City Permit" },
  raleigh_permits:  { label: "Raleigh Permits",    region: "Americas",     type: "City Permit" },
};

// GET /api/etl — return source stats (count per source, last run)
export async function GET() {
  try {
    const countResult = await query(
      `SELECT source_name, COUNT(*) AS count, MAX(last_verified_at) AS last_updated
       FROM projects GROUP BY source_name`,
      []
    );

    const lastRunResult = await query(
      `SELECT source_name, MAX(started_at) AS last_run, status
       FROM etl_runs GROUP BY source_name, status`,
      []
    ).catch(() => ({ rows: [] }));

    const countMap: Record<string, { count: number; last_updated: string }> = {};
    for (const row of countResult.rows) {
      countMap[row.source_name] = { count: parseInt(row.count), last_updated: row.last_updated };
    }

    const runMap: Record<string, string> = {};
    for (const row of lastRunResult.rows) {
      if (row.status === 'success') runMap[row.source_name] = row.last_run;
    }

    const sources = Object.entries(SOURCE_META).map(([key, meta]) => ({
      key,
      ...meta,
      count:        countMap[meta.label]?.count       ?? countMap[key]?.count       ?? 0,
      last_updated: countMap[meta.label]?.last_updated ?? countMap[key]?.last_updated ?? null,
      last_run:     runMap[meta.label] ?? runMap[key] ?? null,
    }));

    return NextResponse.json({ sources });
  } catch (err) {
    console.error('[GET /api/etl]', err);
    return NextResponse.json({ error: 'Internal server error' }, { status: 500 });
  }
}

// POST /api/etl — trigger a scraper run
export async function POST(req: NextRequest) {
  try {
    const body = await req.json().catch(() => ({}));
    const source: string = body.source || 'all';

    if (source !== 'all' && !VALID_SOURCES.includes(source)) {
      return NextResponse.json({ error: 'Invalid source' }, { status: 400 });
    }

    // Fire and forget — spawn ETL as background process
    const { exec } = await import('child_process');
    const cmd = source === 'all'
      ? 'python -m etl.runner'
      : `python -m etl.runner --source ${source}`;

    exec(cmd, { cwd: '/app' }, (err, stdout, stderr) => {
      if (err) console.error('[ETL trigger error]', err.message);
    });

    return NextResponse.json({
      ok: true,
      message: source === 'all' ? 'All scrapers triggered' : `${source} scraper triggered`,
      source,
    });
  } catch (err) {
    console.error('[POST /api/etl]', err);
    return NextResponse.json({ error: 'Internal server error' }, { status: 500 });
  }
}