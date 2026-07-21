import { Component, OnInit } from '@angular/core';
import { CommonModule, DatePipe } from '@angular/common';
import { ActivatedRoute, RouterLink, RouterModule } from '@angular/router';
import { NavbarComponent } from '../navbar/navbar.component';
import { DomSanitizer, SafeHtml } from '@angular/platform-browser';
import { marked } from 'marked';
import { Subject } from 'rxjs';
import { NgxGraphModule, DagreLayout } from '@swimlane/ngx-graph';
import type { Edge, Node } from '@swimlane/ngx-graph';
import type { DagreSettings } from '@swimlane/ngx-graph';
import { CircularsApiService, Circular, Signatory, Expert, ReferenceGraphResponse } from '../services/circulars-api.service';
import { ExpertModalComponent } from '../expert-modal/expert-modal.component';
import { LoginService } from '../services/login.service';

interface ActionItem {
  id: string;
  action_item: string;
  circular_id: string;
  deadline: string;
  persona: string;
  priority: string;
  created_at: string;
  updated_at: string;
}


@Component({
  selector: 'app-circular-detail',
  standalone: true,
  imports: [CommonModule, NavbarComponent, DatePipe, ExpertModalComponent, RouterModule, RouterLink, NgxGraphModule],
  templateUrl: './circular-detail.component.html',
  styleUrl: './circular-detail.component.css'
})
export class CircularDetailComponent implements OnInit {

  circular: Circular | null = null;
  loading = true;
  circularError: 'connection' | 'not_found' | null = null;
  actionItems: ActionItem[] = [];
  actionItemsLoading = false;
  actionItemsError: 'connection' | 'not_found' | null = null;

  summary: string | null = null;
  summaryLoading = false;
  showExpertModal = false;
  signatories: Signatory[] = [];
  signatoriesLoading = false;
  experts: Expert[] = [];
  expertsLoading = false;
  isLoggedIn = false;

  referenceGraph: ReferenceGraphResponse | null = null;
  referenceGraphLoading = false;
  referenceGraphError = false;

  // ngx-graph bindings (derived from `referenceGraph`).
  graphNodes: Node[] = [];
  graphLinks: Edge[] = [];

  // Subject to imperatively trigger ngx-graph's center() after data loads.
  // autoCenter runs once on init (before data arrives) so we need to re-fire it.
  centerTrigger$ = new Subject<void>();

  // ngx-graph v12 requires [layout] to be a DagreLayout *instance* with a
  // .run() method, NOT a plain config object. Settings are assigned to
  // `.settings` after construction. v12 uses `orientation` (LR/RL/TB/BT)
  // and padding fields (nodePadding/rankPadding/edgePadding).
  layoutSettings = new DagreLayout();

  constructor(
    private sanitizer: DomSanitizer,
    private route: ActivatedRoute,
    private api: CircularsApiService,
    private auth: LoginService
  ) {
    this.isLoggedIn = this.auth.isAuthenticated();

    // Configure the Dagre layout: top-to-bottom hierarchy with breathing room.
    // v12 uses `orientation` (LR/RL/TB/BT) instead of `rankDir`. Cast as any
    // because the Orientation enum value type isn't trivially assignable here.
    // NOTE: do NOT set `align` — dagre's balance() throws on Alignment.CENTER
    // because it tries xss['c'][v] but xss only has ul/ur/dl/dr keys.
    // Centering happens at the viewport level via ngx-graph's [autoCenter].
    this.layoutSettings.settings = {
      orientation: 'TB' as any,
      marginX: 30,
      marginY: 30,
      nodePadding: 20,
      edgePadding: 20,
      rankPadding: 80,
    };
  }

  ngOnInit() {
    const id = this.route.snapshot.paramMap.get('id');
    if (id) {
      this.api.getCircularRecord(id).subscribe({
        next: (data) => {
          this.circular = data;
          this.loading = false;
          this.fetchActionItems(id);
          this.fetchSummary(id);
          this.fetchSignatories(id);
          this.fetchExperts(id);
          this.fetchReferenceGraph(id);
        },
        error: (err) => {
          this.loading = false;
          this.circularError = err?.status === 0 ? 'connection' : 'not_found';
        }
      });
    }
  }

  private fetchSignatories(circularId: string) {
    this.signatoriesLoading = true;
    this.api.getSignatories(circularId).subscribe({
      next: (data) => {
        this.signatories = data.signatories;
        this.signatoriesLoading = false;
      },
      error: () => {
        this.signatories = [];
        this.signatoriesLoading = false;
      }
    });
  }

  private fetchExperts(circularId: string) {
    this.expertsLoading = true;
    this.api.getExperts(circularId).subscribe({
      next: (data) => {
        this.experts = data.experts;
        this.expertsLoading = false;
      },
      error: () => {
        this.experts = [];
        this.expertsLoading = false;
      }
    });
  }

  private fetchSummary(circularId: string) {
    this.summaryLoading = true;
    this.api.getSummary(circularId).subscribe({
      next: (data) => {
        this.summary = data.summary;
        this.summaryLoading = false;
      },
      error: () => {
        this.summary = null;
        this.summaryLoading = false;
      }
    });
  }

  private fetchReferenceGraph(circularId: string) {
    this.referenceGraphLoading = true;
    this.referenceGraphError = false;
    this.api.getReferenceGraph(circularId).subscribe({
      next: (graph) => {
        this.referenceGraph = graph;
        this.referenceGraphLoading = false;
        console.log('[reference-graph] loaded', {
          total_nodes: graph.stats.total_nodes,
          total_links: graph.stats.total_links,
          resolved_nodes: graph.stats.resolved_nodes,
          unresolved_nodes: graph.stats.unresolved_nodes,
          max_depth: graph.stats.max_depth,
        });
        this.toGraphModel();

        // Re-center after the layout finishes. autoCenter runs once on init
        // (before data arrives) and never again — we re-trigger here at multiple
        // delays to catch different lifecycle points where the transform settles.
        [0, 50, 200, 500].forEach((delay) => {
          setTimeout(() => this.centerTrigger$.next(), delay);
        });
      },
      error: (err) => {
        this.referenceGraph = null;
        this.graphNodes = [];
        this.graphLinks = [];
        this.referenceGraphLoading = false;
        this.referenceGraphError = true;
        console.warn('[reference-graph] failed to load', err?.status, err?.message);
      }
    });
  }

  /** Convert API response into ngx-graph nodes/edges. */
  private toGraphModel() {
    if (!this.referenceGraph) {
      this.graphNodes = [];
      this.graphLinks = [];
      return;
    }
    const stubId = (label: string) => `stub:${label}`;

    this.graphNodes = this.referenceGraph.nodes.map((n) => ({
      id: n.id ?? stubId(n.label),
      label: n.label,
      // Dimension required by dagre's balance/layout phase — must be set
      // explicitly or the layout throws "Cannot read properties of undefined".
      dimension: { width: 200, height: 78 },
      data: {
        label: n.label,
        title: n.title ?? '',
        date: n.issue_date ?? '',
        exchange: n.exchange ?? null,
        is_root: n.is_root,
        unresolved: n.unresolved,
      },
    }));

    this.graphLinks = this.referenceGraph.links.map((l, i) => ({
      id: `e:${i}`,
      source: l.source,
      target: l.target ?? stubId(l.target_label ?? 'unknown'),
      label: l.label,
      data: { rel_type: l.label, unresolved: l.target === null },
    }));
  }

  // Format an ISO date string ("2020-03-15") as "15 Mar 2020".
  formatDate(iso: string | null | undefined): string {
    if (!iso) return '';
    const d = new Date(iso);
    if (Number.isNaN(d.getTime())) return '';
    return d.toLocaleDateString('en-US', {
      day: 'numeric',
      month: 'short',
      year: 'numeric',
    });
  }

  /** Header color per exchange — applied via inline style in the template. */
  getHeaderColor(exchange: string | null | undefined): string {
    switch ((exchange ?? '').toUpperCase()) {
      case 'SEBI': return '#7c3aed';
      case 'NSE':  return '#dc2626';
      case 'NCL':  return '#0891b2';
      case 'AFD':  return '#ea580c';
      default:     return '#71717a';
    }
  }

  /** Header label — falls back to "Unknown" for unresolved stubs. */
  getExchangeLabel(exchange: string | null | undefined): string {
    const e = (exchange ?? '').trim().toUpperCase();
    return e || 'Unknown';
  }

  /** Edge stroke color per relationship type — applied via inline style. */
  getEdgeColor(relType: string | undefined): string {
    switch ((relType ?? '').toLowerCase()) {
      case 'supersedes': return '#ef4444';
      case 'amends':     return '#f59e0b';
      case 'implements': return '#3b82f6';
      case 'clarifies':  return '#8b5cf6';
      case 'modifies':   return '#10b981';
      case 'rescinds':   return '#dc2626';
      case 'enforces':   return '#0891b2';
      default:           return '#a1a1aa';
    }
  }

  parseMarkdown(text: string | null): SafeHtml {
    if (!text) return '';
    return this.sanitizer.bypassSecurityTrustHtml(marked.parse(text) as string);
  }

  private fetchActionItems(circularId: string) {
    this.actionItemsLoading = true;
    this.actionItemsError = null;
    this.api.getActionItems(circularId).subscribe({
      next: (data) => {
        this.actionItems = data.action_items;
        this.actionItemsLoading = false;
      },
      error: (err) => {
        this.actionItems = [];
        this.actionItemsLoading = false;
        this.actionItemsError = err?.status === 0 ? 'connection' : 'not_found';
      }
    });
  }

  openSource() {
    if (this.circular?.url) {
      window.open(this.circular.url, '_blank');
    }
  }

  openExpertModal() {
    this.showExpertModal = true;
  }

  closeExpertModal() {
    this.showExpertModal = false;
  }

  getDeadlineText(deadline: string): string {
    const today = new Date();
    today.setHours(0, 0, 0, 0);
    const deadlineDate = new Date(deadline);
    deadlineDate.setHours(0, 0, 0, 0);
    const diffMs = deadlineDate.getTime() - today.getTime();
    const diffDays = Math.ceil(diffMs / (1000 * 60 * 60 * 24));

    if (diffDays < 0) {
      return `${Math.abs(diffDays)} days overdue`;
    } else if (diffDays === 0) {
      return 'Due today';
    } else if (diffDays === 1) {
      return 'Due tomorrow';
    } else if (diffDays <= 7) {
      return `Due in ${diffDays} days`;
    } else if (diffDays <= 30) {
      const weeks = Math.floor(diffDays / 7);
      return weeks === 1 ? 'Due in 1 week' : `Due in ${weeks} weeks`;
    } else {
      return `Due in ${Math.floor(diffDays / 30)} months`;
    }
  }
}
