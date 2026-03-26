import { useState, useEffect, useRef, useCallback } from "react";

// ─── API CLIENT ───────────────────────────────────────────────────────────────
const API_BASE = "";

async function apiFetch(path, options = {}) {
  const res = await fetch(`${API_BASE}/api${path}`, {
    ...options,
    headers: { "Content-Type": "application/json", ...options.headers },
    credentials: "include",
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ error: res.statusText }));
    throw Object.assign(new Error(err.error || "API error"), { status: res.status });
  }
  return res.json();
}

const projectsApi = {
  list: (filters = {}) => {
    const params = new URLSearchParams();
    Object.entries(filters).forEach(([k, v]) => {
      if (v !== undefined && v !== "" && v !== "All") params.set(k, String(v));
    });
    return apiFetch(`/projects?${params}`);
  },
};

const etlApi = {
  sources: () => apiFetch("/etl"),
  trigger: (source = "all") => apiFetch("/etl", { method: "POST", body: JSON.stringify({ source }) }),
};

const leadsApi = {
  board: () => apiFetch("/leads"),
  save:  (projectId, status = "Discovery") =>
    apiFetch("/leads", { method: "POST", body: JSON.stringify({ project_id: projectId, status }) }),
  move:  (leadId, status) =>
    apiFetch("/leads", { method: "PUT", body: JSON.stringify({ lead_id: leadId, status }) }),
};

// ─── MOCK DATA (fallback when API unreachable) ────────────────────────────────
const MOCK_PROJECTS = [
  { id:"1", title:"Dubai Metro Phase 4 Extension", value_usd:4200000000, location_display:"Dubai, UAE", sector:"Transport", stage:"Tender", region:"Middle East", score:87, timeline_display:"2025–2029", last_verified_at:"2025-02-18", source_name:"TED EU", description:"34km elevated metro extension with 20 new stations connecting outer suburbs to the existing Red and Green lines.", milestones:[{label:"Feasibility",completed:true},{label:"EIA Approved",completed:true},{label:"Tender Open",completed:false},{label:"Award",completed:false}], active_milestone:2, match_score:35, budget_score:30, timeline_score:22, stakeholders:[{role:"Owner",name:"RTA Dubai"},{role:"Architect",name:"Atkins Global"}], geojson:{coordinates:[55.2708,25.2048]} },
  { id:"2", title:"Sydney Harbour Tunnel Duplication", value_usd:3100000000, location_display:"Sydney, Australia", sector:"Infrastructure", stage:"Planning", region:"Asia Pacific", score:74, timeline_display:"2026–2031", last_verified_at:"2025-02-20", source_name:"AusTender", description:"Second undersea tunnel beneath Sydney Harbour to relieve congestion.", milestones:[{label:"Concept",completed:true},{label:"Env Review",completed:false},{label:"Design",completed:false},{label:"Tender",completed:false}], active_milestone:1, match_score:30, budget_score:26, timeline_score:18, stakeholders:[{role:"Owner",name:"Transport NSW"},{role:"Engineer",name:"WSP"}], geojson:{coordinates:[151.2093,-33.8688]} },
  { id:"3", title:"Nairobi Green Energy Hub", value_usd:890000000, location_display:"Nairobi, Kenya", sector:"Energy", stage:"Awarded", region:"Africa", score:91, timeline_display:"2025–2027", last_verified_at:"2025-02-22", source_name:"KPLC Portal", description:"400MW solar–wind complex with grid-scale battery storage for Nairobi metro area.", milestones:[{label:"Award Signed",completed:true},{label:"Site Prep",completed:true},{label:"Construction",completed:false},{label:"Commission",completed:false}], active_milestone:1, match_score:36, budget_score:32, timeline_score:23, stakeholders:[{role:"Owner",name:"KPLC"},{role:"Contractor",name:"China Power Engineering"}], geojson:{coordinates:[36.8219,-1.2921]} },
  { id:"4", title:"Frankfurt Digital Campus", value_usd:620000000, location_display:"Frankfurt, Germany", sector:"Commercial", stage:"Tender", region:"Europe", score:68, timeline_display:"2025–2028", last_verified_at:"2025-02-15", source_name:"TED EU", description:"180,000m² AI-enabled smart building campus, targeting LEED Platinum certification.", milestones:[{label:"Planning",completed:true},{label:"Design",completed:true},{label:"Tender",completed:false},{label:"Award",completed:false}], active_milestone:2, match_score:27, budget_score:24, timeline_score:17, stakeholders:[{role:"Owner",name:"Allianz Real Estate"},{role:"Architect",name:"Snøhetta"}], geojson:{coordinates:[8.6821,50.1109]} },
  { id:"5", title:"São Paulo Flood Resilience Program", value_usd:2700000000, location_display:"São Paulo, Brazil", sector:"Water", stage:"Planning", region:"Americas", score:79, timeline_display:"2026–2033", last_verified_at:"2025-02-10", source_name:"SABESP", description:"300km tunnel drainage network with real-time flood monitoring and retention basins.", milestones:[{label:"Feasibility",completed:true},{label:"Federal Approval",completed:false},{label:"Design",completed:false},{label:"Procurement",completed:false}], active_milestone:0, match_score:32, budget_score:28, timeline_score:19, stakeholders:[{role:"Owner",name:"SABESP"},{role:"Engineer",name:"Jacobs"}], geojson:{coordinates:[-46.6333,-23.5505]} },
  { id:"6", title:"Riyadh Sports Boulevard", value_usd:1450000000, location_display:"Riyadh, Saudi Arabia", sector:"Sport & Leisure", stage:"Awarded", region:"Middle East", score:95, timeline_display:"2024–2026", last_verified_at:"2025-02-24", source_name:"ROSHN Official", description:"12km linear sports district with 8 stadiums, cycling tracks and retail as part of Vision 2030.", milestones:[{label:"Award Signed",completed:true},{label:"Site Prep",completed:true},{label:"Structure",completed:true},{label:"Fit-Out",completed:false}], active_milestone:2, match_score:38, budget_score:33, timeline_score:24, stakeholders:[{role:"Owner",name:"ROSHN"},{role:"Architect",name:"Populous"},{role:"Main Contractor",name:"Saudi Binladin Group"}], geojson:{coordinates:[46.6753,24.7136]} },
  { id:"7", title:"Singapore Cross-Island MRT", value_usd:5800000000, location_display:"Singapore", sector:"Transport", stage:"Tender", region:"Asia Pacific", score:82, timeline_display:"2025–2032", last_verified_at:"2025-02-19", source_name:"LTA Singapore", description:"50km east-west MRT line via deep tunnels through the Central Catchment Nature Reserve.", milestones:[{label:"Env Cleared",completed:true},{label:"Detailed Design",completed:true},{label:"Tender Open",completed:false},{label:"Award",completed:false}], active_milestone:2, match_score:33, budget_score:29, timeline_score:20, stakeholders:[{role:"Owner",name:"LTA Singapore"},{role:"Engineer",name:"AECOM"}], geojson:{coordinates:[103.8198,1.3521]} },
  { id:"8", title:"Lagos Waterfront Regeneration", value_usd:760000000, location_display:"Lagos, Nigeria", sector:"Mixed Use", stage:"Planning", region:"Africa", score:61, timeline_display:"2026–2030", last_verified_at:"2025-02-12", source_name:"LASG", description:"4,000 affordable housing units with retail, parks and a new ferry terminus on Victoria Island.", milestones:[{label:"Concept",completed:true},{label:"Master Plan",completed:false},{label:"Approvals",completed:false},{label:"Tender",completed:false}], active_milestone:1, match_score:24, budget_score:21, timeline_score:16, stakeholders:[{role:"Owner",name:"LASG"},{role:"Architect",name:"Kéré Architecture"}], geojson:{coordinates:[3.3792,6.5244]} },
];

const MOCK_BOARD = {
  Discovery:  [MOCK_PROJECTS[0], MOCK_PROJECTS[1]],
  Qualifying: [MOCK_PROJECTS[5]],
  Bidding:    [MOCK_PROJECTS[2]],
  Won:        [],
  Lost:       [],
};

// ─── CONSTANTS ────────────────────────────────────────────────────────────────
const KANBAN_STAGES  = ["Discovery","Qualifying","Bidding","Won","Lost"];
const SECTOR_COLORS  = { Transport:"#0EA5E9", Infrastructure:"#8B5CF6", Energy:"#F59E0B", Commercial:"#10B981", Water:"#06B6D4", "Sport & Leisure":"#F97316", "Mixed Use":"#EC4899", Residential:"#84CC16", Healthcare:"#EF4444", Education:"#A78BFA" };
const STAGE_COLORS   = { Planning:"#94A3B8", Tender:"#F59E0B", Awarded:"#10B981", "Under Construction":"#0EA5E9" };
const MODES = [
  { key:"construction", label:"Construction", accent:"#F59E0B", bg:"#0A0F1E" },
  { key:"creative",     label:"Creative",     accent:"#EC4899", bg:"#0D0A1E" },
  { key:"software",     label:"Software",     accent:"#0EA5E9", bg:"#050D1A" },
];

const fmt = (n) => { const v = parseFloat(n); return (!v || isNaN(v)) ? "N/A" : v >= 1e9 ? `$${(v/1e9).toFixed(1)}B` : v >= 1e6 ? `$${(v/1e6).toFixed(0)}M` : v >= 1e3 ? `$${(v/1e3).toFixed(0)}K` : `$${v}`; };
const scoreColor = (s) => s >= 80 ? "#10B981" : s >= 60 ? "#F59E0B" : "#EF4444";

// ─── UI PRIMITIVES ────────────────────────────────────────────────────────────
function Spinner({ color="#F59E0B" }) {
  return (
    <div style={{ display:"flex", alignItems:"center", justifyContent:"center", padding:60 }}>
      <div style={{ width:36, height:36, border:`3px solid #1E293B`, borderTop:`3px solid ${color}`, borderRadius:"50%", animation:"spin 0.8s linear infinite" }} />
      <style>{`@keyframes spin{to{transform:rotate(360deg)}}`}</style>
    </div>
  );
}

function ErrorBanner({ message, onRetry }) {
  return (
    <div style={{ background:"#1E293B", border:"1px solid #EF444440", borderRadius:12, padding:"16px 20px", display:"flex", alignItems:"center", justifyContent:"space-between", gap:12, marginBottom:20 }}>
      <div>
        <div style={{ color:"#EF4444", fontWeight:700, fontSize:13 }}>⚠ API Unavailable — showing demo data</div>
        <div style={{ color:"#64748B", fontSize:11, marginTop:2 }}>{message}</div>
      </div>
      {onRetry && <button onClick={onRetry} style={{ padding:"6px 12px", background:"#EF444420", border:"1px solid #EF444440", borderRadius:7, color:"#EF4444", fontSize:11, fontWeight:600, cursor:"pointer", flexShrink:0 }}>Retry</button>}
    </div>
  );
}

function ScoreBadge({ score }) {
  return (
    <div style={{ position:"relative", width:50, height:50, flexShrink:0 }}>
      <svg width="50" height="50" viewBox="0 0 52 52">
        <circle cx="26" cy="26" r="22" fill="none" stroke="#1E293B" strokeWidth="4" />
        <circle cx="26" cy="26" r="22" fill="none" stroke={scoreColor(score)} strokeWidth="4"
          strokeDasharray={`${(score/100)*138.2} 138.2`} strokeLinecap="round" transform="rotate(-90 26 26)" />
      </svg>
      <span style={{ position:"absolute", inset:0, display:"flex", alignItems:"center", justifyContent:"center", fontSize:11, fontWeight:700, color:scoreColor(score) }}>{score}</span>
    </div>
  );
}

function ProgressBar({ label, value, max, color }) {
  return (
    <div style={{ marginBottom:8 }}>
      <div style={{ display:"flex", justifyContent:"space-between", fontSize:11, color:"#94A3B8", marginBottom:3 }}>
        <span>{label}</span><span style={{color}}>{value}/{max}</span>
      </div>
      <div style={{ background:"#1E293B", borderRadius:4, height:6, overflow:"hidden" }}>
        <div style={{ width:`${Math.min(100,(value/max)*100)}%`, height:"100%", background:color, borderRadius:4, transition:"width 0.8s" }} />
      </div>
    </div>
  );
}

function Timeline({ milestones=[], activeMs=0 }) {
  if (!milestones.length) return null;
  return (
    <div style={{ display:"flex", alignItems:"flex-start", marginTop:16, overflowX:"auto", paddingBottom:4 }}>
      {milestones.map((m, i) => (
        <div key={i} style={{ display:"flex", alignItems:"center", flex: i < milestones.length-1 ? 1 : 0, minWidth:0 }}>
          <div style={{ display:"flex", flexDirection:"column", alignItems:"center", minWidth:70 }}>
            <div style={{ width:26, height:26, borderRadius:"50%", background: i<=activeMs ? "#F59E0B" : "#1E293B", border:`2px solid ${i<=activeMs?"#F59E0B":"#334155"}`, display:"flex", alignItems:"center", justifyContent:"center", fontSize:10, fontWeight:700, color: i<=activeMs ? "#0A0F1E":"#475569", flexShrink:0 }}>
              {i<=activeMs ? "✓" : i+1}
            </div>
            <span style={{ fontSize:9, color: i<=activeMs?"#F59E0B":"#475569", marginTop:5, textAlign:"center", lineHeight:1.3, maxWidth:66 }}>{m.label||m}</span>
          </div>
          {i < milestones.length-1 && (
            <div style={{ flex:1, height:2, background: i<activeMs?"#F59E0B":"#1E293B", margin:"0 4px", marginBottom:20 }} />
          )}
        </div>
      ))}
    </div>
  );
}

// ─── PROJECT CARD ─────────────────────────────────────────────────────────────
function ProjectCard({ project, onView, onTrack, tracking }) {
  const [h, setH] = useState(false);
  const sc = SECTOR_COLORS[project.sector]||"#F59E0B";
  const stc = STAGE_COLORS[project.stage]||"#94A3B8";
  return (
    <div onMouseEnter={()=>setH(true)} onMouseLeave={()=>setH(false)}
      style={{ background:"linear-gradient(135deg,#0F172A,#0A0F1E)", border:`1px solid ${h?"#F59E0B40":"#1E293B"}`, borderRadius:14, padding:20, transition:"all 0.25s", boxShadow:h?"0 8px 32px #F59E0B10":"none", position:"relative", overflow:"hidden" }}>
      <div style={{ position:"absolute", top:0, left:0, width:3, height:"100%", background:sc, borderRadius:"14px 0 0 14px" }} />
      <div style={{ paddingLeft:12 }}>
        <div style={{ display:"flex", justifyContent:"space-between", alignItems:"flex-start", gap:10 }}>
          <div style={{ flex:1, minWidth:0 }}>
            <div style={{ display:"flex", gap:6, marginBottom:8, flexWrap:"wrap" }}>
              <span style={{ fontSize:9, fontWeight:700, padding:"2px 9px", borderRadius:20, background:`${sc}20`, color:sc }}>{project.sector}</span>
              <span style={{ fontSize:9, fontWeight:700, padding:"2px 9px", borderRadius:20, background:`${stc}20`, color:stc }}>{project.stage}</span>
            </div>
            <h3 style={{ fontSize:14, fontWeight:700, color:"#F1F5F9", margin:0, lineHeight:1.4 }}>{project.title}</h3>
            <div style={{ fontSize:11, color:"#64748B", marginTop:3 }}>📍 {project.location_display} · {project.timeline_display}</div>
          </div>
          <ScoreBadge score={project.score} />
        </div>
        <div style={{ fontSize:21, fontWeight:800, color:"#F59E0B", marginTop:12, fontFamily:"monospace" }}>{fmt(project.value_usd)}</div>
        <div style={{ marginTop:10, display:"grid", gridTemplateColumns:"1fr 1fr", gap:6 }}>
          <div style={{ fontSize:10, color:"#64748B" }}>Source<br/><span style={{ color:"#94A3B8", fontWeight:600 }}>{project.source_name}</span></div>
          <div style={{ fontSize:10, color:"#64748B" }}>Verified<br/><span style={{ color:"#94A3B8", fontWeight:600 }}>{project.last_verified_at?.slice(0,10)}</span></div>
        </div>
        <div style={{ marginTop:12, display:"flex", gap:8 }}>
          <button onClick={()=>onView(project)} style={{ flex:1, padding:"8px 0", borderRadius:8, background:"#1E293B", border:"none", color:"#94A3B8", fontSize:12, fontWeight:600, cursor:"pointer" }}>View Details</button>
          <button onClick={()=>onTrack(project)} disabled={tracking===project.id}
            style={{ flex:1, padding:"8px 0", borderRadius:8, background:"#F59E0B15", border:"1px solid #F59E0B40", color:"#F59E0B", fontSize:12, fontWeight:600, cursor:"pointer", opacity:tracking===project.id?0.5:1 }}>
            {tracking===project.id?"Adding…":"+ Track Lead"}
          </button>
        </div>
      </div>
    </div>
  );
}

// ─── PROJECT MODAL ────────────────────────────────────────────────────────────
function ProjectDetailMap({ lat, lng, title }) {
  const canvasRef = useRef(null);
  useEffect(() => {
    if (!lat || !lng || !canvasRef.current) return;
    const canvas = canvasRef.current;
    const ctx = canvas.getContext("2d");
    const W = canvas.width, H = canvas.height;
    // Simple world map grid background
    ctx.fillStyle = "#0A0F1E"; ctx.fillRect(0,0,W,H);
    ctx.strokeStyle = "#1E293B"; ctx.lineWidth = 0.5;
    for (let i=0;i<=12;i++){const x=i/12*W;ctx.beginPath();ctx.moveTo(x,0);ctx.lineTo(x,H);ctx.stroke();}
    for (let i=0;i<=6;i++){const y=i/6*H;ctx.beginPath();ctx.moveTo(0,y);ctx.lineTo(W,y);ctx.stroke();}
    // Plot pin
    const x = (lng+180)/360*W;
    const y = (90-lat)/180*H;
    // Pulse ring
    ctx.beginPath();ctx.arc(x,y,22,0,Math.PI*2);ctx.fillStyle="#F59E0B15";ctx.fill();
    ctx.beginPath();ctx.arc(x,y,14,0,Math.PI*2);ctx.fillStyle="#F59E0B30";ctx.fill();
    // Pin body
    const g = ctx.createRadialGradient(x-3,y-3,0,x,y,10);
    g.addColorStop(0,"#FCD34D");g.addColorStop(1,"#F59E0B");
    ctx.beginPath();ctx.arc(x,y,10,0,Math.PI*2);ctx.fillStyle=g;ctx.fill();
    ctx.strokeStyle="#FCD34D";ctx.lineWidth=1.5;ctx.stroke();
    // Label
    ctx.fillStyle="#F1F5F9";ctx.font="bold 10px 'DM Sans',sans-serif";
    ctx.textAlign="center";ctx.textBaseline="bottom";
    ctx.fillStyle="#0A0F1E90";
    const lw = ctx.measureText(title.slice(0,28)).width+16;
    ctx.fillRect(x-lw/2, y-36, lw, 18);
    ctx.fillStyle="#F1F5F9";
    ctx.fillText(title.slice(0,28)+(title.length>28?"…":""), x, y-20);
  }, [lat, lng, title]);
  return (
    <div style={{ borderRadius:12, overflow:"hidden", border:"1px solid #1E293B", position:"relative" }}>
      <canvas ref={canvasRef} width={640} height={200} style={{ width:"100%", display:"block" }} />
      <div style={{ position:"absolute", bottom:8, right:10, fontSize:9, color:"#475569" }}>
        {lat?.toFixed(4)}, {lng?.toFixed(4)}
      </div>
    </div>
  );
}

function ProjectModal({ project, onClose, onTrack }) {
  if (!project) return null;
  const sc  = SECTOR_COLORS[project.sector] || "#F59E0B";
  const stc = STAGE_COLORS[project.stage]   || "#94A3B8";
  const lat = project.geojson?.coordinates?.[1] ?? project.lat;
  const lng = project.geojson?.coordinates?.[0] ?? project.lng;

  return (
    <div style={{ position:"fixed", inset:0, background:"rgba(0,0,0,0.75)", zIndex:100, overflowY:"auto", padding:"24px 0" }} onClick={onClose}>
      <div style={{ background:"#070E1A", border:"1px solid #1E293B", borderRadius:20, maxWidth:760, width:"calc(100% - 32px)", margin:"0 auto", overflow:"hidden" }} onClick={e=>e.stopPropagation()}>

        {/* Header */}
        <div style={{ background:"#0F172A", borderBottom:"1px solid #1E293B", padding:"24px 28px 20px" }}>
          <div style={{ display:"flex", justifyContent:"space-between", alignItems:"flex-start", gap:12 }}>
            <div style={{ flex:1, minWidth:0 }}>
              <div style={{ display:"flex", gap:7, marginBottom:10, flexWrap:"wrap" }}>
                <span style={{ fontSize:10, fontWeight:700, padding:"3px 10px", borderRadius:20, background:`${sc}20`, color:sc }}>{project.sector}</span>
                <span style={{ fontSize:10, fontWeight:700, padding:"3px 10px", borderRadius:20, background:`${stc}20`, color:stc }}>{project.stage}</span>
                <span style={{ fontSize:10, fontWeight:700, padding:"3px 10px", borderRadius:20, background:"#1E293B", color:"#64748B" }}>{project.source_name}</span>
              </div>
              <h2 style={{ fontSize:20, fontWeight:800, color:"#F1F5F9", margin:"0 0 6px", lineHeight:1.3 }}>{project.title}</h2>
              <div style={{ display:"flex", gap:16, flexWrap:"wrap" }}>
                <span style={{ color:"#64748B", fontSize:12 }}>📍 {project.location_display}</span>
                {project.timeline_display && <span style={{ color:"#64748B", fontSize:12 }}>🗓 {project.timeline_display}</span>}
              </div>
            </div>
            <button onClick={onClose} style={{ background:"#1E293B", border:"none", color:"#94A3B8", width:36, height:36, borderRadius:9, cursor:"pointer", fontSize:20, flexShrink:0, display:"flex", alignItems:"center", justifyContent:"center" }}>×</button>
          </div>
        </div>

        <div style={{ padding:"24px 28px" }}>

          {/* KPI row */}
          <div style={{ display:"grid", gridTemplateColumns:"repeat(3,1fr)", gap:12, marginBottom:24 }}>
            {[
              ["Contract Value", fmt(project.value_usd), "#F59E0B"],
              ["AI Score",       `${project.score || "—"}/100`, scoreColor(project.score||0)],
              ["Last Verified",  project.last_verified_at?.slice(0,10) || "—", "#0EA5E9"],
            ].map(([l,v,c])=>(
              <div key={l} style={{ background:"#0A0F1E", borderRadius:10, padding:"14px 16px", border:"1px solid #1E293B" }}>
                <div style={{ fontSize:10, color:"#475569", fontWeight:600, letterSpacing:"0.06em" }}>{l.toUpperCase()}</div>
                <div style={{ fontSize:18, fontWeight:800, color:c, marginTop:6, fontFamily:"monospace" }}>{v}</div>
              </div>
            ))}
          </div>

          {/* Description */}
          {project.description && (
            <div style={{ marginBottom:24 }}>
              <div style={{ fontSize:11, fontWeight:700, color:"#475569", letterSpacing:"0.08em", marginBottom:10 }}>PROJECT OVERVIEW</div>
              <p style={{ color:"#94A3B8", fontSize:13, lineHeight:1.8, margin:0, background:"#0F172A", borderRadius:10, padding:"14px 16px", border:"1px solid #1E293B" }}>
                {project.description}
              </p>
            </div>
          )}

          {/* Map pin */}
          <div style={{ marginBottom:24 }}>
            <div style={{ fontSize:11, fontWeight:700, color:"#475569", letterSpacing:"0.08em", marginBottom:10 }}>LOCATION</div>
            {(lat && lng)
              ? <ProjectDetailMap lat={lat} lng={lng} title={project.title} />
              : <div style={{ background:"#0F172A", border:"1px solid #1E293B", borderRadius:12, padding:"20px 16px", display:"flex", alignItems:"center", gap:12 }}>
                  <span style={{ fontSize:24 }}>📍</span>
                  <div>
                    <div style={{ fontSize:14, fontWeight:600, color:"#F1F5F9" }}>{project.location_display || "Location not specified"}</div>
                    <div style={{ fontSize:11, color:"#475569", marginTop:3 }}>Precise coordinates not available for this project</div>
                  </div>
                </div>
            }
          </div>

          {/* Milestones */}
          {project.milestones?.length > 0 && (
            <div style={{ marginBottom:24 }}>
              <div style={{ fontSize:11, fontWeight:700, color:"#475569", letterSpacing:"0.08em", marginBottom:10 }}>PROJECT MILESTONES</div>
              <Timeline milestones={project.milestones} activeMs={project.active_milestone} />
            </div>
          )}

          {/* Stakeholders */}
          {project.stakeholders?.length > 0 && (
            <div style={{ marginBottom:24 }}>
              <div style={{ fontSize:11, fontWeight:700, color:"#475569", letterSpacing:"0.08em", marginBottom:12 }}>STAKEHOLDERS</div>
              <div style={{ display:"grid", gridTemplateColumns:"repeat(auto-fill,minmax(160px,1fr))", gap:10 }}>
                {project.stakeholders.map((s,i)=>(
                  <div key={i} style={{ background:"#0F172A", borderRadius:10, padding:"12px 14px", border:"1px solid #1E293B" }}>
                    <div style={{ fontSize:9, color:"#475569", fontWeight:700, letterSpacing:"0.06em" }}>{(s.role||"").toUpperCase()}</div>
                    <div style={{ fontSize:13, color:"#F1F5F9", fontWeight:600, marginTop:5 }}>{s.name||"TBD"}</div>
                  </div>
                ))}
              </div>
            </div>
          )}

          {/* AI Scoring breakdown */}
          <div style={{ marginBottom:24 }}>
            <div style={{ fontSize:11, fontWeight:700, color:"#475569", letterSpacing:"0.08em", marginBottom:12 }}>AI SCORING BREAKDOWN</div>
            <div style={{ background:"#0F172A", borderRadius:10, padding:"16px", border:"1px solid #1E293B" }}>
              <ProgressBar label="Sector & Region Match" value={project.match_score}    max={40} color="#10B981" />
              <ProgressBar label="Budget Alignment"      value={project.budget_score}   max={35} color="#0EA5E9" />
              <ProgressBar label="Timeline Fit"          value={project.timeline_score} max={25} color="#F59E0B" />
            </div>
          </div>

          {/* Actions */}
          <div style={{ display:"flex", gap:10, flexWrap:"wrap" }}>
            {project.source_url && (
              <a href={project.source_url} target="_blank" rel="noopener noreferrer"
                style={{ flex:1, minWidth:140, display:"inline-flex", alignItems:"center", justifyContent:"center", gap:8, padding:"11px 0", background:"#0F172A", border:"1px solid #334155", borderRadius:10, color:"#94A3B8", fontSize:13, fontWeight:600, textDecoration:"none" }}>
                🔗 Source Listing
              </a>
            )}
            <button onClick={()=>{onTrack(project);onClose();}}
              style={{ flex:2, minWidth:200, padding:"11px 0", borderRadius:10, background:"#F59E0B", border:"none", color:"#0A0F1E", fontSize:14, fontWeight:800, cursor:"pointer" }}>
              + Track This Lead
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}

// ─── KANBAN CARD ──────────────────────────────────────────────────────────────
function KanbanCard({ lead, isDragging }) {
  const sc = SECTOR_COLORS[lead.sector]||"#F59E0B";
  return (
    <div style={{ background:"#0F172A", border:"1px solid #1E293B", borderRadius:10, padding:13, marginBottom:9, cursor:"grab", opacity:isDragging?0.4:1, transition:"opacity 0.15s", borderLeft:`3px solid ${sc}` }}>
      <div style={{ fontSize:12, fontWeight:700, color:"#F1F5F9", marginBottom:5, lineHeight:1.4 }}>{lead.title}</div>
      <div style={{ display:"flex", justifyContent:"space-between", alignItems:"center" }}>
        <span style={{ fontSize:10, color:"#64748B" }}>{lead.location_display}</span>
        <span style={{ fontSize:12, fontWeight:700, color:"#F59E0B" }}>{fmt(lead.value_usd)}</span>
      </div>
      <div style={{ marginTop:8, display:"flex", alignItems:"center", gap:6 }}>
        <span style={{ fontSize:9, padding:"2px 7px", borderRadius:20, background:`${sc}20`, color:sc, fontWeight:600 }}>{lead.sector}</span>
        <ScoreBadge score={lead.score} />
      </div>
      {lead.reminders?.length > 0 && (
        <div style={{ marginTop:7, fontSize:10, color:"#F59E0B" }}>🔔 {lead.reminders[0].title}</div>
      )}
    </div>
  );
}

// ─── MAP CANVAS ───────────────────────────────────────────────────────────────
function MapCanvas({ projects }) {
  const canvasRef = useRef(null);
  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const ctx = canvas.getContext("2d");
    const W = canvas.width, H = canvas.height;
    const toXY = (lat, lng) => ({ x:(lng+180)/360*W, y:(90-lat)/180*H });
    ctx.clearRect(0,0,W,H);
    ctx.fillStyle="#0F172A"; ctx.fillRect(0,0,W,H);
    ctx.strokeStyle="#1E293B"; ctx.lineWidth=0.5;
    for(let i=0;i<=12;i++){const x=i/12*W;ctx.beginPath();ctx.moveTo(x,0);ctx.lineTo(x,H);ctx.stroke();}
    for(let i=0;i<=6;i++){const y=i/6*H;ctx.beginPath();ctx.moveTo(0,y);ctx.lineTo(W,y);ctx.stroke();}
    projects.forEach(p => {
      const c = p.geojson?.coordinates; if(!c) return;
      const {x,y} = toXY(c[1],c[0]);
      const r = Math.max(8,Math.min(22,(p.value_usd||0)/2e8));
      const col = SECTOR_COLORS[p.sector]||"#F59E0B";
      ctx.beginPath();ctx.arc(x,y,r+7,0,Math.PI*2);ctx.fillStyle=`${col}15`;ctx.fill();
      const g=ctx.createRadialGradient(x-r*.3,y-r*.3,0,x,y,r);
      g.addColorStop(0,`${col}FF`);g.addColorStop(1,`${col}70`);
      ctx.beginPath();ctx.arc(x,y,r,0,Math.PI*2);ctx.fillStyle=g;ctx.fill();
      ctx.fillStyle="#0A0F1E";ctx.font="bold 9px monospace";ctx.textAlign="center";ctx.textBaseline="middle";
      ctx.fillText(p.score,x,y);
    });
  }, [projects]);
  return (
    <div style={{ position:"relative", borderRadius:14, overflow:"hidden", border:"1px solid #1E293B" }}>
      <canvas ref={canvasRef} width={760} height={360} style={{ width:"100%", display:"block" }} />
      <div style={{ position:"absolute", top:14, left:14, fontSize:11, color:"#94A3B8", background:"#0A0F1E90", padding:"5px 12px", borderRadius:8 }}>
        🌍 {projects.length} live projects · bubble = value · label = AI score
      </div>
    </div>
  );
}

// ─── MAIN APP ─────────────────────────────────────────────────────────────────
export default function CSTGlobal() {
  const [view, setView]         = useState("feed");
  const [mode, setMode]         = useState(MODES[0]);
  const [projects, setProjects] = useState([]);
  const [board, setBoard]       = useState({ Discovery:[], Qualifying:[], Bidding:[], Won:[], Lost:[] });
  const [loadingFeed, setLoadingFeed] = useState(false);
  const [loadingCRM, setLoadingCRM]   = useState(false);
  const [feedError, setFeedError]     = useState(null);
  const [crmError, setCrmError]       = useState(null);
  const [apiOnline, setApiOnline]     = useState(true);
  const [selected, setSelected]       = useState(null);
  const [tracking, setTracking]       = useState(null);
  const [dragging, setDragging]       = useState(null);
  const [sidebarOpen, setSidebarOpen] = useState(false);
  const [biddableOnly, setBiddableOnly] = useState(false);
  const [toast, setToast]             = useState(null);
  const [filters, setFilters]         = useState({ region:"All", sector:"All", stage:"All", source:"All", q:"" });
  const [sources, setSources]         = useState([]);
  const [loadingSources, setLoadingSources] = useState(false);
  const [triggering, setTriggering]   = useState(null);

  const accent = mode.accent;

  const notify = useCallback((msg, color) => {
    setToast({ msg, color: color||accent });
    setTimeout(()=>setToast(null), 2800);
  }, [accent]);

  // ── Fetch projects ─────────────────────────────────────────
  const fetchProjects = useCallback(async () => {
    setLoadingFeed(true); setFeedError(null);
    try {
      const stageFilter = biddableOnly ? undefined : (filters.stage !== 'All' ? filters.stage : undefined);
      const biddableStages = biddableOnly ? 'Planning,Tender' : undefined;
      const res = await projectsApi.list({ region:filters.region, sector:filters.sector, stage:stageFilter, biddable:biddableStages, source:filters.source, q:filters.q, limit:200 });
      setProjects(res.data);
      setApiOnline(true);
    } catch(err) {
      setApiOnline(false);
      const mock = MOCK_PROJECTS.filter(p => {
        if(filters.region!=="All" && p.region!==filters.region) return false;
        if(filters.sector!=="All" && p.sector!==filters.sector) return false;
        if(filters.stage!=="All"  && p.stage!==filters.stage)   return false;
        if(filters.q && !p.title.toLowerCase().includes(filters.q.toLowerCase())) return false;
        return true;
      });
      setProjects(mock);
      setFeedError(err.message);
    } finally { setLoadingFeed(false); }
  }, [filters, biddableOnly]);

  // ── Fetch ETL sources ──────────────────────────────────────
  const fetchSources = useCallback(async () => {
    setLoadingSources(true);
    try {
      const res = await etlApi.sources();
      setSources(res.sources || []);
    } catch {
      // Fallback mock sources when API offline
      setSources([
        { key:"world_bank",       label:"World Bank",           region:"Global",       type:"Development Project",    count:0, last_run:null },
        { key:"contracts_finder", label:"Contracts Finder",     region:"Europe",       type:"Public Tender",          count:0, last_run:null },
        { key:"uk_planning",      label:"UK Planning Portal",   region:"Europe",       type:"Planning Application",   count:0, last_run:null },
        { key:"nsw_eplanning",    label:"NSW ePlanning",        region:"Asia Pacific", type:"Planning Application",   count:0, last_run:null },
        { key:"nyc_permits",      label:"NYC DOB",              region:"Americas",     type:"City Permit",            count:0, last_run:null },
        { key:"chicago_permits",  label:"Chicago Permits",      region:"Americas",     type:"City Permit",            count:0, last_run:null },
        { key:"la_permits",       label:"LA Building & Safety", region:"Americas",     type:"City Permit",            count:0, last_run:null },
        { key:"houston_permits",  label:"Houston Permits",      region:"Americas",     type:"City Permit",            count:0, last_run:null },
        { key:"philly_permits",   label:"Philadelphia L&I",     region:"Americas",     type:"City Permit",            count:0, last_run:null },
        { key:"usace",            label:"USACE",                region:"Americas",     type:"Federal Infrastructure", count:0, last_run:null },
        { key:"philly_arcgis",   label:"Philadelphia L&I",    region:"Americas",     type:"City Permit",            count:0, last_run:null },
        { key:"denver_permits",  label:"Dallas Permits",      region:"Americas",     type:"City Permit",            count:0, last_run:null },
        { key:"sf_permits",      label:"SF Building Permits", region:"Americas",     type:"City Permit",            count:0, last_run:null },
        { key:"boston_permits",   label:"Boston Permits",      region:"Americas",     type:"City Permit",            count:0, last_run:null },
        { key:"sj_permits",      label:"San Jose Permits",    region:"Americas",     type:"City Permit",            count:0, last_run:null },
        { key:"baltimore_permits",label:"Baltimore Permits",   region:"Americas",     type:"City Permit",            count:0, last_run:null },
        { key:"ted_eu",           label:"TED EU",               region:"Europe",       type:"Public Tender",          count:0, last_run:null },
        { key:"sam_gov",          label:"SAM.gov",              region:"Americas",     type:"Public Tender",          count:0, last_run:null },
      ]);
    } finally { setLoadingSources(false); }
  }, []);

  // ── Trigger scraper ────────────────────────────────────────
  const handleTrigger = useCallback(async (sourceKey) => {
    setTriggering(sourceKey);
    try {
      await etlApi.trigger(sourceKey);
      notify(`${sourceKey === "all" ? "All scrapers" : sourceKey} triggered — data will appear in ~60s`);
      setTimeout(() => { fetchSources(); fetchProjects(); }, 8000);
    } catch(err) {
      notify("Trigger failed: " + err.message, "#EF4444");
    } finally { setTriggering(null); }
  }, [notify, fetchSources, fetchProjects]);

  // ── Fetch board ────────────────────────────────────────────
  const fetchBoard = useCallback(async () => {
    setLoadingCRM(true); setCrmError(null);
    try {
      const res = await leadsApi.board();
      setBoard(res.data);
    } catch(err) {
      setBoard(MOCK_BOARD);
      setCrmError("Using demo board — " + err.message);
    } finally { setLoadingCRM(false); }
  }, []);

  useEffect(() => { fetchProjects(); }, [fetchProjects]);
  useEffect(() => { if(view==="crm") fetchBoard(); }, [view, fetchBoard]);
  useEffect(() => { if(view==="sources") fetchSources(); }, [view, fetchSources]);

  // ── Track lead ─────────────────────────────────────────────
  const handleTrack = useCallback(async (project) => {
    setTracking(project.id);
    try {
      await leadsApi.save(project.id, "Discovery");
      notify(`Added to Discovery`);
      if(view==="crm") fetchBoard();
    } catch {
      setBoard(b => {
        const flat = Object.values(b).flat();
        if(flat.find(l => (l.project_id||l.id)===project.id)) { notify("Already tracked","#EF4444"); return b; }
        notify("Added to Discovery (offline)");
        const newLead = { ...project, lead_id:`local-${Date.now()}`, project_id:project.id, status:"Discovery", reminders:[] };
        return { ...b, Discovery:[newLead,...b.Discovery] };
      });
    } finally { setTracking(null); }
  }, [view, fetchBoard, notify]);

  // ── Drag and drop ──────────────────────────────────────────
  const handleDrop = useCallback(async (e, toStage) => {
    e.preventDefault();
    if(!dragging || dragging.fromStage===toStage) { setDragging(null); return; }
    const { lead, fromStage } = dragging;
    setBoard(b => ({
      ...b,
      [fromStage]: b[fromStage].filter(l=>(l.lead_id||l.id)!==(lead.lead_id||lead.id)),
      [toStage]:   [{ ...lead, status:toStage }, ...b[toStage]],
    }));
    notify(`Moved to ${toStage}`);
    setDragging(null);
    try { await leadsApi.move(lead.lead_id||lead.id, toStage); }
    catch { notify("Sync failed — move may not persist","#EF4444"); }
  }, [dragging, notify]);

  const displayProjects = projects;
  const totalValue = displayProjects.reduce((s,p)=>s+(Number(p.value_usd)||0), 0);
  const avgScore   = projects.length ? Math.round(projects.reduce((s,p)=>s+p.score,0)/projects.length) : 0;
  const regions    = ["All",...new Set(MOCK_PROJECTS.map(p=>p.region))];
  const sectors    = ["All",...new Set(MOCK_PROJECTS.map(p=>p.sector))];
  const stages     = ["All","Planning","Tender","Awarded"];

  const nav = [
    { key:"feed",    icon:"⚡", label:"Discovery Feed" },
    { key:"map",     icon:"🌍", label:"Global Map"     },
    { key:"crm",     icon:"📋", label:"Lead Tracker"   },
    { key:"sources", icon:"🔌", label:"Data Sources"   },
    { key:"portal",  icon:"👥", label:"Client Portal"  },
  ];

  const sel = { background:"#0F172A", border:"1px solid #1E293B", borderRadius:9, padding:"8px 12px", color:"#94A3B8", fontSize:12, outline:"none", cursor:"pointer" };

  return (
    <div style={{ minHeight:"100vh", background:mode.bg, fontFamily:"'DM Sans','Segoe UI',sans-serif", color:"#F1F5F9" }}>
      <style>{`
        @media (max-width: 768px) {
          .cst-sidebar { transform: translateX(-100%); transition: transform 0.25s ease; }
          .cst-sidebar.open { transform: translateX(0); }
          .cst-main { margin-left: 0 !important; }
          .cst-topbar { padding: 10px 14px !important; }
          .cst-topbar input { min-width: 100px !important; }
          .cst-grid-2 { grid-template-columns: 1fr !important; }
          .cst-grid-3 { grid-template-columns: 1fr 1fr !important; }
          .cst-grid-5 { grid-template-columns: 1fr 1fr !important; }
          .cst-kpi-grid { grid-template-columns: repeat(2,1fr) !important; }
          .cst-mobile-toggle { display: flex !important; }
          .cst-api-indicator { left: 14px !important; }
          .cst-content-pad { padding: 14px !important; }
        }
        @media (max-width: 480px) {
          .cst-grid-3 { grid-template-columns: 1fr !important; }
          .cst-grid-5 { grid-template-columns: 1fr !important; }
          .cst-kpi-grid { grid-template-columns: repeat(2,1fr) !important; }
          .cst-topbar { flex-wrap: wrap; gap: 6px !important; }
        }
        .cst-overlay { position: fixed; inset: 0; background: rgba(0,0,0,0.6); z-index: 49; }
        .cst-mobile-toggle { display: none; }
      `}</style>

      {/* Toast */}
      {toast && (
        <div style={{ position:"fixed", top:20, right:20, background:"#0F172A", border:`1px solid ${toast.color}60`, borderRadius:10, padding:"11px 18px", fontSize:13, fontWeight:600, color:toast.color, zIndex:300, boxShadow:`0 0 20px ${toast.color}20`, maxWidth:340 }}>
          {toast.msg}
        </div>
      )}

      {/* API indicator */}
      <div className="cst-api-indicator" style={{ position:"fixed", bottom:14, left:228, zIndex:50, display:"flex", alignItems:"center", gap:5, fontSize:10, color:"#475569" }}>
        <div style={{ width:6, height:6, borderRadius:"50%", background:apiOnline?"#10B981":"#F59E0B" }} />
        {apiOnline?"API Connected":"Demo Mode"}
      </div>

      {/* Sidebar overlay for mobile */}
      {sidebarOpen && <div className="cst-overlay" onClick={()=>setSidebarOpen(false)} style={{ position:"fixed", inset:0, background:"rgba(0,0,0,0.6)", zIndex:49 }} />}

      {/* Sidebar */}
      <div className={`cst-sidebar${sidebarOpen?" open":""}`} style={{ position:"fixed", left:0, top:0, bottom:0, width:220, background:"#070E1A", borderRight:"1px solid #1E293B", display:"flex", flexDirection:"column", zIndex:50 }}>
        <div style={{ padding:"22px 20px 18px", borderBottom:"1px solid #1E293B" }}>
          <div style={{ display:"flex", alignItems:"center", gap:10 }}>
            <div style={{ width:34, height:34, borderRadius:9, background:`linear-gradient(135deg,${accent},${accent}70)`, display:"flex", alignItems:"center", justifyContent:"center", fontSize:16, fontWeight:900 }}>C</div>
            <div>
              <div style={{ fontSize:16, fontWeight:900, letterSpacing:"-0.02em" }}>CST<span style={{color:accent}}>Global</span></div>
              <div style={{ fontSize:9, color:"#475569", letterSpacing:"0.06em" }}>LEAD INTELLIGENCE</div>
            </div>
          </div>
        </div>

        <div style={{ padding:"12px 14px", borderBottom:"1px solid #1E293B" }}>
          <div style={{ fontSize:9, color:"#475569", fontWeight:700, letterSpacing:"0.08em", marginBottom:7 }}>INDUSTRY MODE</div>
          {MODES.map(m=>(
            <button key={m.key} onClick={()=>setMode(m)} style={{ width:"100%", padding:"6px 10px", borderRadius:7, background:mode.key===m.key?`${m.accent}20`:"transparent", border:`1px solid ${mode.key===m.key?m.accent+"50":"transparent"}`, color:mode.key===m.key?m.accent:"#64748B", fontSize:12, fontWeight:600, cursor:"pointer", textAlign:"left", marginBottom:2, transition:"all 0.15s" }}>
              {m.label}
            </button>
          ))}
        </div>

        <nav style={{ flex:1, padding:"12px 10px" }}>
          {nav.map(item=>(
            <button key={item.key} onClick={()=>setView(item.key)} style={{ width:"100%", display:"flex", alignItems:"center", gap:9, padding:"9px 11px", borderRadius:9, background:view===item.key?`${accent}18`:"transparent", border:`1px solid ${view===item.key?accent+"40":"transparent"}`, color:view===item.key?accent:"#64748B", fontSize:13, fontWeight:600, cursor:"pointer", marginBottom:3, transition:"all 0.15s" }}>
              <span style={{fontSize:14}}>{item.icon}</span>{item.label}
            </button>
          ))}
        </nav>

        <div style={{ padding:14, borderTop:"1px solid #1E293B" }}>
          <div style={{ display:"flex", alignItems:"center", gap:10 }}>
            <div style={{ width:32, height:32, borderRadius:"50%", background:`linear-gradient(135deg,${accent},${accent}60)`, display:"flex", alignItems:"center", justifyContent:"center", fontSize:11, fontWeight:800, color:"#0A0F1E" }}>AM</div>
            <div>
              <div style={{ fontSize:12, fontWeight:700 }}>Alex Mercer</div>
              <span style={{ fontSize:9, padding:"1px 6px", borderRadius:10, background:`${accent}30`, color:accent, fontWeight:700 }}>PRO</span>
            </div>
          </div>
        </div>
      </div>

      {/* Main */}
      <div className="cst-main" style={{ marginLeft:220, minHeight:"100vh" }}>
        {/* Topbar */}
        <div className="cst-topbar" style={{ position:"sticky", top:0, background:`${mode.bg}E8`, backdropFilter:"blur(16px)", borderBottom:"1px solid #1E293B", padding:"12px 26px", display:"flex", alignItems:"center", gap:10, zIndex:40, flexWrap:"wrap" }}>
          <button className="cst-mobile-toggle" onClick={()=>setSidebarOpen(o=>!o)} style={{ alignItems:"center", justifyContent:"center", width:34, height:34, background:"#0F172A", border:"1px solid #1E293B", borderRadius:8, color:"#94A3B8", fontSize:18, cursor:"pointer", flexShrink:0 }}>☰</button>
          <input value={filters.q} onChange={e=>setFilters(f=>({...f,q:e.target.value}))} placeholder="Search projects…"
            style={{ flex:1, minWidth:160, maxWidth:300, background:"#0F172A", border:"1px solid #1E293B", borderRadius:9, padding:"8px 14px", color:"#F1F5F9", fontSize:13, outline:"none" }} />
          <select value={filters.region} onChange={e=>setFilters(f=>({...f,region:e.target.value}))} style={sel}>{regions.map(o=><option key={o}>{o}</option>)}</select>
          <select value={filters.sector} onChange={e=>setFilters(f=>({...f,sector:e.target.value}))} style={sel}>{sectors.map(o=><option key={o}>{o}</option>)}</select>
          <select value={filters.stage}  onChange={e=>setFilters(f=>({...f,stage:e.target.value}))}  style={sel}>{stages.map(o=><option key={o}>{o}</option>)}</select>
          <select value={filters.source} onChange={e=>setFilters(f=>({...f,source:e.target.value}))} style={sel}>
            <option value="All">All Sources</option>
            <optgroup label="City Permits (US)">
              <option value="NYC DOB">NYC DOB</option>
              <option value="Chicago Permits">Chicago</option>
              <option value="LA Building &amp; Safety">Los Angeles</option>
              <option value="Houston Permits">Houston</option>
              <option value="Seattle Permits">Seattle</option>
              <option value="Philadelphia L&amp;I">Philadelphia</option>
              <option value="Dallas Permits">Dallas</option>
              <option value="SF Building Permits">San Francisco</option>
              <option value="USACE">USACE</option>
            </optgroup>
            <optgroup label="Planning Applications">
              <option value="UK Planning Portal">UK Planning</option>
              <option value="NSW ePlanning">NSW Planning</option>
            </optgroup>
            <optgroup label="Public Procurement">
              <option value="World Bank">World Bank</option>
              <option value="Contracts Finder">Contracts Finder</option>
              <option value="TED EU">TED EU</option>
              <option value="SAM.gov">SAM.gov</option>
            </optgroup>
          </select>
          <button
            onClick={()=>setBiddableOnly(b=>!b)}
            title="Show only biddable projects (Planning & Tender)"
            style={{ padding:"7px 13px", borderRadius:8, border:`1px solid ${biddableOnly?accent+"80":"#1E293B"}`, background:biddableOnly?`${accent}18`:"#0F172A", color:biddableOnly?accent:"#64748B", fontSize:11, fontWeight:700, cursor:"pointer", whiteSpace:"nowrap", display:"flex", alignItems:"center", gap:5, transition:"all 0.15s" }}>
            🎯 {biddableOnly?"Biddable Only":"All Stages"}
          </button>
          <div style={{ marginLeft:"auto", fontSize:12, color:"#475569", whiteSpace:"nowrap" }}>
            <span style={{color:accent,fontWeight:700}}>{displayProjects.length}</span> projects · <span style={{color:accent,fontWeight:700}}>{fmt(totalValue)}</span>
          </div>
        </div>

        <div className="cst-content-pad" style={{ padding:26 }}>

          {/* DISCOVERY FEED */}
          {view==="feed" && (
            <>
              {feedError && <ErrorBanner message={feedError} onRetry={fetchProjects} />}
              <div className="cst-kpi-grid" style={{ display:"grid", gridTemplateColumns:"repeat(4,1fr)", gap:14, marginBottom:24 }}>
                {[
                  {label:"Active Projects", value:projects.length, icon:"📂", color:"#0EA5E9"},
                  {label:"Total Pipeline",  value:fmt(totalValue), icon:"💰", color:"#F59E0B"},
                  {label:"Avg AI Score",    value:avgScore||"—",   icon:"🎯", color:"#10B981"},
                  {label:"In Tender",       value:projects.filter(p=>p.stage==="Tender").length, icon:"📋", color:"#8B5CF6"},
                ].map(k=>(
                  <div key={k.label} style={{ background:"#0F172A", border:"1px solid #1E293B", borderRadius:13, padding:"18px 20px" }}>
                    <div style={{ fontSize:20, marginBottom:7 }}>{k.icon}</div>
                    <div style={{ fontSize:24, fontWeight:800, color:k.color, fontFamily:"monospace" }}>{k.value}</div>
                    <div style={{ fontSize:11, color:"#64748B", marginTop:3 }}>{k.label}</div>
                  </div>
                ))}
              </div>
              {loadingFeed
                ? <Spinner color={accent} />
                : projects.length===0
                  ? <div style={{ textAlign:"center", color:"#475569", paddingTop:80 }}>No projects match your filters</div>
                  : <div className="cst-grid-2" style={{ display:"grid", gridTemplateColumns:"repeat(2,1fr)", gap:16 }}>
                      {displayProjects.map(p=><ProjectCard key={p.id} project={p} onView={setSelected} onTrack={handleTrack} tracking={tracking} />)}
                    </div>
              }
            </>
          )}

          {/* MAP */}
          {view==="map" && (
            <>
              <h2 style={{ fontSize:18, fontWeight:800, marginBottom:18 }}>🌍 Global Project Map</h2>
              <MapCanvas projects={projects.filter(p=>p.geojson)} />
              <div className="cst-grid-3" style={{ marginTop:18, display:"grid", gridTemplateColumns:"repeat(3,1fr)", gap:12 }}>
                {["Middle East","Asia Pacific","Europe","Americas","Africa","Central Asia"].map(r=>{
                  const rp = projects.filter(p=>p.region===r);
                  return (
                    <div key={r} style={{ background:"#0F172A", border:"1px solid #1E293B", borderRadius:12, padding:14 }}>
                      <div style={{ fontSize:12, fontWeight:700 }}>{r}</div>
                      <div style={{ fontSize:20, fontWeight:800, color:accent, fontFamily:"monospace" }}>{rp.length} <span style={{ fontSize:12, color:"#64748B", fontFamily:"inherit" }}>projects</span></div>
                      <div style={{ fontSize:11, color:"#64748B" }}>{fmt(rp.reduce((s,p)=>s+(Number(p.value_usd)||0),0))} total</div>
                    </div>
                  );
                })}
              </div>
            </>
          )}

          {/* CRM */}
          {view==="crm" && (
            <>
              <div style={{ display:"flex", alignItems:"center", justifyContent:"space-between", marginBottom:18 }}>
                <h2 style={{ fontSize:18, fontWeight:800, margin:0 }}>📋 Lead Tracker — Kanban</h2>
                <button onClick={fetchBoard} style={{ padding:"6px 12px", background:"#1E293B", border:"1px solid #334155", borderRadius:8, color:"#94A3B8", fontSize:11, cursor:"pointer" }}>↻ Refresh</button>
              </div>
              {crmError && <ErrorBanner message={crmError} />}
              {loadingCRM
                ? <Spinner color={accent} />
                : <div className="cst-grid-5" style={{ display:"grid", gridTemplateColumns:"repeat(5,1fr)", gap:12, alignItems:"start" }}>
                    {KANBAN_STAGES.map(stage=>(
                      <div key={stage}
                        style={{ background:"#0F172A", border:"1px solid #1E293B", borderRadius:13, padding:12, minHeight:180 }}
                        onDragOver={e=>e.preventDefault()} onDrop={e=>handleDrop(e,stage)}>
                        <div style={{ display:"flex", justifyContent:"space-between", alignItems:"center", marginBottom:12 }}>
                          <span style={{ fontSize:10, fontWeight:800, color:"#94A3B8", letterSpacing:"0.08em" }}>{stage.toUpperCase()}</span>
                          <span style={{ fontSize:10, width:20, height:20, borderRadius:"50%", background:`${accent}30`, color:accent, display:"flex", alignItems:"center", justifyContent:"center", fontWeight:700 }}>{(board[stage]||[]).length}</span>
                        </div>
                        {(board[stage]||[]).map(lead=>(
                          <div key={lead.lead_id||lead.id} draggable
                            onDragStart={()=>setDragging({lead,fromStage:stage})}
                            onDragEnd={()=>setDragging(null)}>
                            <KanbanCard lead={lead} isDragging={dragging?.lead&&(dragging.lead.lead_id||dragging.lead.id)===(lead.lead_id||lead.id)} />
                          </div>
                        ))}
                        {!(board[stage]||[]).length && (
                          <div style={{ textAlign:"center", color:"#334155", fontSize:11, paddingTop:20 }}>Drop leads here</div>
                        )}
                      </div>
                    ))}
                  </div>
              }
            </>
          )}

          {/* DATA SOURCES */}
          {view==="sources" && (
            <>
              <div style={{ display:"flex", alignItems:"center", justifyContent:"space-between", marginBottom:22 }}>
                <div>
                  <h2 style={{ fontSize:18, fontWeight:800, margin:0 }}>🔌 Data Sources</h2>
                  <p style={{ fontSize:12, color:"#475569", marginTop:4 }}>Manage scrapers and see live data counts per source</p>
                </div>
                <button onClick={()=>handleTrigger("all")} disabled={triggering==="all"}
                  style={{ padding:"9px 20px", background:accent, border:"none", borderRadius:9, color:"#0A0F1E", fontWeight:800, fontSize:13, cursor:"pointer", opacity:triggering==="all"?0.5:1 }}>
                  {triggering==="all" ? "Running…" : "▶ Run All Scrapers"}
                </button>
              </div>

              {/* Source type legend */}
              <div style={{ display:"flex", gap:12, marginBottom:20, flexWrap:"wrap" }}>
                {[["Planning Application","#10B981"],["Public Tender","#0EA5E9"],["Development Project","#8B5CF6"],["City Permit","#F97316"],["Federal Infrastructure","#EC4899"]].map(([t,c])=>(
                  <div key={t} style={{ display:"flex", alignItems:"center", gap:6, fontSize:11, color:"#94A3B8" }}>
                    <div style={{ width:8, height:8, borderRadius:"50%", background:c }} />{t}
                  </div>
                ))}
              </div>

              {loadingSources
                ? <Spinner color={accent} />
                : <div style={{ display:"grid", gridTemplateColumns:"repeat(2,1fr)", gap:14 }}>
                    {sources.map(src => {
                      const typeColor = src.type==="Planning Application" ? "#10B981" : src.type==="Public Tender" ? "#0EA5E9" : src.type==="City Permit" ? "#F97316" : src.type==="Federal Infrastructure" ? "#EC4899" : "#8B5CF6";
                      const isRunning = triggering===src.key;
                      return (
                        <div key={src.key} style={{ background:"#0F172A", border:"1px solid #1E293B", borderRadius:14, padding:20, position:"relative", overflow:"hidden" }}>
                          <div style={{ position:"absolute", top:0, left:0, width:3, height:"100%", background:typeColor, borderRadius:"14px 0 0 14px" }} />
                          <div style={{ paddingLeft:12 }}>
                            <div style={{ display:"flex", justifyContent:"space-between", alignItems:"flex-start", marginBottom:12 }}>
                              <div>
                                <div style={{ fontSize:15, fontWeight:800, color:"#F1F5F9" }}>{src.label}</div>
                                <div style={{ display:"flex", gap:7, marginTop:5, flexWrap:"wrap" }}>
                                  <span style={{ fontSize:9, padding:"2px 8px", borderRadius:20, background:`${typeColor}20`, color:typeColor, fontWeight:700 }}>{src.type}</span>
                                  <span style={{ fontSize:9, padding:"2px 8px", borderRadius:20, background:"#1E293B", color:"#64748B", fontWeight:600 }}>{src.region}</span>
                                </div>
                              </div>
                              <div style={{ textAlign:"right" }}>
                                <div style={{ fontSize:28, fontWeight:900, color:accent, fontFamily:"monospace" }}>{(src.count||0).toLocaleString()}</div>
                                <div style={{ fontSize:10, color:"#64748B" }}>projects</div>
                              </div>
                            </div>
                            <div style={{ display:"grid", gridTemplateColumns:"1fr 1fr", gap:8, marginBottom:14 }}>
                              <div style={{ background:"#0A0F1E", borderRadius:8, padding:"8px 10px" }}>
                                <div style={{ fontSize:9, color:"#475569" }}>LAST RUN</div>
                                <div style={{ fontSize:11, color:"#94A3B8", fontWeight:600, marginTop:2 }}>{src.last_run ? new Date(src.last_run).toLocaleDateString() : "Never"}</div>
                              </div>
                              <div style={{ background:"#0A0F1E", borderRadius:8, padding:"8px 10px" }}>
                                <div style={{ fontSize:9, color:"#475569" }}>LAST UPDATED</div>
                                <div style={{ fontSize:11, color:"#94A3B8", fontWeight:600, marginTop:2 }}>{src.last_updated ? new Date(src.last_updated).toLocaleDateString() : "—"}</div>
                              </div>
                            </div>
                            <div style={{ display:"flex", gap:8 }}>
                              <button onClick={()=>handleTrigger(src.key)} disabled={!!triggering}
                                style={{ flex:1, padding:"8px 0", borderRadius:8, background:`${accent}15`, border:`1px solid ${accent}40`, color:accent, fontSize:12, fontWeight:700, cursor:"pointer", opacity:triggering?0.5:1 }}>
                                {isRunning ? "Running…" : "▶ Run Now"}
                              </button>
                              <button onClick={()=>setFilters(f=>({...f,source:src.label,region:"All",sector:"All",stage:"All"})) || setView("feed")}
                                style={{ flex:1, padding:"8px 0", borderRadius:8, background:"#1E293B", border:"none", color:"#94A3B8", fontSize:12, fontWeight:600, cursor:"pointer" }}>
                                View Projects →
                              </button>
                            </div>
                          </div>
                        </div>
                      );
                    })}
                  </div>
              }

              {/* Source descriptions */}
              <div style={{ marginTop:24, background:"#0F172A", border:"1px solid #1E293B", borderRadius:14, padding:20 }}>
                <div style={{ fontSize:13, fontWeight:700, marginBottom:14 }}>📖 Source Guide</div>
                <div style={{ display:"grid", gridTemplateColumns:"1fr 1fr", gap:12 }}>
                  {[
                    { label:"NYC DOB",             desc:"New York City Dept of Buildings permit filings. Best municipal dataset in the US — new buildings and major alterations with job values, addresses, and owner names. Free Socrata API.", color:"#F97316" },
                    { label:"Chicago Permits",      desc:"City of Chicago building permits. Strong commercial and mixed-use coverage across all 77 neighbourhoods. New construction and renovation, with reported costs. Free Socrata API.", color:"#F97316" },
                    { label:"LA Building & Safety", desc:"Los Angeles Dept of Building & Safety. Large city with diverse project types — commercial, residential, entertainment. Includes valuation and applicant names. Free Socrata API.", color:"#F97316" },
                    { label:"Houston Permits",      desc:"City of Houston building permits. Strong energy sector and commercial coverage. Includes owner and contractor names for direct outreach. Free Socrata API.", color:"#F97316" },
                    { label:"Philadelphia L&I",     desc:"Philadelphia Licenses & Inspections permits. Dense city with good commercial and mixed-use coverage. Returns contractor names alongside project values.", color:"#F97316" },
                    { label:"USACE",                desc:"US Army Corps of Engineers civil infrastructure contracts — dams, levees, ports, waterways, flood control. Uses SAM.gov API, shares daily rate limit.", color:"#EC4899" },
                    { label:"UK Planning Portal",   desc:"Private development applications across England & Wales. Captures commercial, residential, and mixed-use projects before they go to tender. Free, no API key required.", color:"#10B981" },
                    { label:"NSW ePlanning",        desc:"Development applications lodged with Sydney and New South Wales councils. Strong coverage of major commercial and infrastructure projects. Free, no API key required.", color:"#10B981" },
                    { label:"World Bank",           desc:"Active international development projects worldwide. Excellent for infrastructure, transport, water, and energy in Africa, Asia, and Americas. Free, no API key required.", color:"#8B5CF6" },
                    { label:"Contracts Finder",     desc:"UK public procurement notices from central and local government. Covers construction, facilities management, and civil engineering contracts. Free, no API key required.", color:"#0EA5E9" },
                    { label:"TED EU",               desc:"Official EU procurement journal covering public tenders across all 27 member states. Currently returning 404 — API endpoint may have changed.", color:"#0EA5E9" },
                    { label:"SAM.gov",              desc:"US federal procurement — construction, infrastructure, and facilities contracts. Rate limited to 1,000 calls/day. Resets at midnight UTC.", color:"#0EA5E9" },
                  ].map(s=>(
                    <div key={s.label} style={{ display:"flex", gap:10 }}>
                      <div style={{ width:3, borderRadius:2, background:s.color, flexShrink:0 }} />
                      <div>
                        <div style={{ fontSize:12, fontWeight:700, color:"#F1F5F9", marginBottom:3 }}>{s.label}</div>
                        <div style={{ fontSize:11, color:"#64748B", lineHeight:1.6 }}>{s.desc}</div>
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            </>
          )}

          {/* CLIENT PORTAL */}
          {view==="portal" && (
            <>
              <h2 style={{ fontSize:18, fontWeight:800, marginBottom:18 }}>👥 Real-Time Client Portal</h2>
              <div style={{ display:"grid", gridTemplateColumns:"2fr 1fr", gap:20 }}>
                <div>
                  {projects.filter(p=>p.stage==="Awarded").slice(0,2).map(p=>(
                    <div key={p.id} style={{ background:"#0F172A", border:"1px solid #1E293B", borderRadius:14, padding:20, marginBottom:16 }}>
                      <div style={{ display:"flex", justifyContent:"space-between", alignItems:"center" }}>
                        <div>
                          <div style={{ fontSize:14, fontWeight:700 }}>{p.title}</div>
                          <div style={{ fontSize:11, color:"#64748B" }}>{p.location_display}</div>
                        </div>
                        <span style={{ fontSize:10, padding:"3px 10px", borderRadius:20, background:"#10B98120", color:"#10B981", fontWeight:700 }}>AWARDED</span>
                      </div>
                      <Timeline milestones={p.milestones} activeMs={p.active_milestone} />
                      <div style={{ marginTop:14, borderTop:"1px solid #1E293B", paddingTop:14 }}>
                        <div style={{ fontSize:10, color:"#64748B", marginBottom:8 }}>MILESTONE COMMENTS</div>
                        {["Site cleared and mobilisation complete — ahead of schedule.","Structural survey completed — no surprises found."].map((c,i)=>(
                          <div key={i} style={{ display:"flex", gap:9, marginBottom:9 }}>
                            <div style={{ width:26, height:26, borderRadius:"50%", background:`${accent}30`, display:"flex", alignItems:"center", justifyContent:"center", fontSize:9, fontWeight:700, color:accent, flexShrink:0 }}>PM</div>
                            <div style={{ background:"#0A0F1E", borderRadius:8, padding:"9px 12px", fontSize:12, color:"#94A3B8", lineHeight:1.5 }}>{c}</div>
                          </div>
                        ))}
                        <div style={{ display:"flex", gap:8, marginTop:10 }}>
                          <input placeholder="Add milestone update…" style={{ flex:1, background:"#0A0F1E", border:"1px solid #1E293B", borderRadius:8, padding:"8px 12px", color:"#F1F5F9", fontSize:12, outline:"none" }} />
                          <button style={{ padding:"8px 14px", background:accent, border:"none", borderRadius:8, color:"#0A0F1E", fontWeight:700, fontSize:12, cursor:"pointer" }}>Post</button>
                        </div>
                      </div>
                    </div>
                  ))}
                  {projects.filter(p=>p.stage==="Awarded").length===0 && (
                    <div style={{ color:"#475569", textAlign:"center", padding:60 }}>No awarded projects in current filter</div>
                  )}
                </div>
                <div>
                  <div style={{ background:"#0F172A", border:"1px solid #1E293B", borderRadius:13, padding:18, marginBottom:14 }}>
                    <div style={{ fontSize:13, fontWeight:700, marginBottom:12 }}>Active Stakeholders</div>
                    {["Client: ROSHN","PM: Turner & Townsend","Architect: Populous","QS: AECOM"].map((s,i)=>(
                      <div key={i} style={{ display:"flex", alignItems:"center", gap:9, padding:"9px 0", borderBottom:i<3?"1px solid #1E293B":"none" }}>
                        <div style={{ width:28, height:28, borderRadius:"50%", background:`${accent}20`, display:"flex", alignItems:"center", justifyContent:"center", fontSize:11, color:accent, fontWeight:700 }}>{s[s.indexOf(":")+2]}</div>
                        <span style={{ fontSize:12, color:"#94A3B8" }}>{s}</span>
                        <div style={{ marginLeft:"auto", width:6, height:6, borderRadius:"50%", background:"#10B981" }} />
                      </div>
                    ))}
                  </div>
                  <div style={{ background:"#0F172A", border:"1px solid #1E293B", borderRadius:13, padding:18 }}>
                    <div style={{ fontSize:13, fontWeight:700, marginBottom:12 }}>Upcoming Reminders</div>
                    {[["Site Visit","Mar 5","#F59E0B"],["Tender Deadline","Mar 14","#EF4444"],["Progress Meeting","Mar 20","#10B981"]].map(([t,d,c])=>(
                      <div key={t} style={{ display:"flex", justifyContent:"space-between", padding:"9px 0", borderBottom:"1px solid #1E293B40" }}>
                        <div>
                          <div style={{ fontSize:12, fontWeight:600 }}>{t}</div>
                          <div style={{ fontSize:10, color:"#64748B" }}>{d}</div>
                        </div>
                        <div style={{ width:4, borderRadius:4, background:c, alignSelf:"stretch", marginLeft:8 }} />
                      </div>
                    ))}
                  </div>
                </div>
              </div>
            </>
          )}

        </div>
      </div>

      <ProjectModal project={selected} onClose={()=>setSelected(null)} onTrack={handleTrack} />
    </div>
  );
}
