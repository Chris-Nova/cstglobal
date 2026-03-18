// lib/api-client.ts
// Typed fetch wrapper for all CSTGlobal API routes.
// Used by the React frontend instead of the hardcoded PROJECTS array.

export interface Project {
  id:                   string;
  title:                string;
  description:          string | null;
  value_usd:            number;
  value_currency:       string;
  location_display:     string;
  location_country:     string;
  region:               string;
  sector:               string;
  stage:                string;
  timeline_display:     string | null;
  milestones:           Array<{ label: string; completed: boolean }>;
  active_milestone:     number;
  source_name:          string;
  source_url:           string | null;
  tender_document_url:  string | null;
  last_verified_at:     string;
  first_seen_at:        string;
  geojson:              { type: string; coordinates: [number, number] } | null;
  score:                number;
  match_score:          number;
  budget_score:         number;
  timeline_score:       number;
  stakeholders:         Array<{ role: string; name: string; confirmed: boolean }>;
}

export interface Lead {
  lead_id:        string;
  status:         "Discovery" | "Qualifying" | "Bidding" | "Won" | "Lost";
  notes:          string | null;
  bid_value_usd:  number | null;
  probability_pct: number | null;
  tags:           string[];
  position:       number;
  tracked_since:  string;
  project_id:     string;
  title:          string;
  value_usd:      number;
  location_display: string;
  region:         string;
  sector:         string;
  project_stage:  string;
  score:          number;
  reminders:      Array<{ id: string; title: string; remind_at: string; is_sent: boolean }>;
}

export interface KanbanBoard {
  Discovery:  Lead[];
  Qualifying: Lead[];
  Bidding:    Lead[];
  Won:        Lead[];
  Lost:       Lead[];
}

export interface ProjectFilters {
  region?:   string;
  sector?:   string;
  stage?:    string;
  minValue?: number;
  maxValue?: number;
  q?:        string;
  page?:     number;
  limit?:    number;
  sortBy?:   "value_usd" | "last_verified_at" | "created_at" | "total_score";
  sortDir?:  "asc" | "desc";
}

export interface PaginatedResponse<T> {
  data: T[];
  meta: { total: number; page: number; limit: number; pages: number };
}

// ── Base fetch (adds auth header automatically) ──────────────
async function apiFetch<T>(
  path: string,
  options: RequestInit = {}
): Promise<T> {
  const res = await fetch(`/api${path}`, {
    ...options,
    headers: {
      "Content-Type": "application/json",
      ...options.headers,
    },
    credentials: "include",   // send session cookie
  });

  if (!res.ok) {
    const err = await res.json().catch(() => ({ error: res.statusText }));
    throw Object.assign(new Error(err.error || "API error"), { status: res.status, data: err });
  }

  return res.json();
}

// ── Projects ─────────────────────────────────────────────────
export const projectsApi = {
  list: (filters: ProjectFilters = {}): Promise<PaginatedResponse<Project>> => {
    const params = new URLSearchParams();
    Object.entries(filters).forEach(([k, v]) => {
      if (v !== undefined && v !== "" && v !== "All") params.set(k, String(v));
    });
    return apiFetch(`/projects?${params}`);
  },

  get: (id: string): Promise<{ data: Project }> =>
    apiFetch(`/projects/${id}`),
};

// ── Leads / CRM ───────────────────────────────────────────────
export const leadsApi = {
  board: (): Promise<{ data: KanbanBoard }> =>
    apiFetch("/leads"),

  save: (projectId: string, status = "Discovery"): Promise<{ data: Lead }> =>
    apiFetch("/leads", {
      method: "POST",
      body: JSON.stringify({ project_id: projectId, status }),
    }),

  move: (leadId: string, status: string): Promise<{ data: Lead }> =>
    apiFetch(`/leads/${leadId}`, {
      method: "PUT",
      body: JSON.stringify({ status }),
    }),

  update: (leadId: string, payload: Partial<Lead>): Promise<{ data: Lead }> =>
    apiFetch(`/leads/${leadId}`, {
      method: "PUT",
      body: JSON.stringify(payload),
    }),

  addReminder: (leadId: string, title: string, remindAt: string) =>
    apiFetch(`/leads/${leadId}/reminders`, {
      method: "POST",
      body: JSON.stringify({ title, remind_at: remindAt }),
    }),
};

// ── Scores ────────────────────────────────────────────────────
export const scoresApi = {
  calculate: (projectId: string) =>
    apiFetch("/scores", {
      method: "POST",
      body: JSON.stringify({ project_id: projectId }),
    }),
};

// ── Health ────────────────────────────────────────────────────
export const healthApi = {
  check: () => apiFetch("/health"),
};
