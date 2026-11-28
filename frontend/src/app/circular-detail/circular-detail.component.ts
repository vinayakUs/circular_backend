import { Component, ElementRef, NgZone, OnDestroy, OnInit, ViewChild } from '@angular/core';
import { CommonModule, DatePipe } from '@angular/common';
import { ActivatedRoute, RouterLink, RouterModule } from '@angular/router';
import { DomSanitizer, SafeHtml } from '@angular/platform-browser';
import { marked } from 'marked';
import type cytoscape from 'cytoscape';
import { CircularsApiService, Circular, Signatory, Expert, DirectReference } from '../services/circulars-api.service';
import { ExpertModalComponent } from '../expert-modal/expert-modal.component';
import { AuthService } from '../services/auth.service';
import { normalizeSource } from '../util/source.util';

interface ActionItem {
  id: string;
  action_item: string;
  circular_id: string;
  deadline: string | null;
  days_until_deadline: number | null;
  relative_deadline: string | null;
  created_at: string;
  updated_at: string;
}


@Component({
  selector: 'app-circular-detail',
  standalone: true,
  imports: [CommonModule, DatePipe, ExpertModalComponent, RouterModule, RouterLink],
  templateUrl: './circular-detail.component.html',
  styleUrl: './circular-detail.component.css'
})
export class CircularDetailComponent implements OnInit, OnDestroy {

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
  selectedNodeId: string | null = null;
  graphNotice = '';
  private readonly pending = new Set<string>();
  private readonly expanded = new Set<string>();
  private cy: cytoscape.Core | null = null;
  private graphResizeObserver: ResizeObserver | null = null;
  private graphContainer?: HTMLDivElement;
  private graphDestroyed = false;

  @ViewChild('cyContainer') set cyContainer(ref: ElementRef<HTMLDivElement> | undefined) {
    this.graphContainer = ref?.nativeElement;
    if (this.graphContainer) {
      void this.createGraph(this.graphContainer).catch(() => {
        this.graphNotice = 'Could not display the reference graph.';
      });
    }
  }

  private readonly handleGraphResize = () => {
    if (!this.graphDestroyed) {
      this.cy?.resize();
    }
  };

  constructor(
    private sanitizer: DomSanitizer,
    private route: ActivatedRoute,
    private api: CircularsApiService,
    private auth: AuthService,
    private zone: NgZone
  ) {}

  get isLoggedIn(): boolean {
    return this.auth.isAuthenticated();
  }

  ngOnInit() {
    this.route.paramMap.subscribe((params) => {
      const id = params.get('id');
      if (!id) return;

      // Reset state when navigating to a different circular so the UI
      // doesn't briefly show stale data from the previous visit.
      this.circular = null;
      this.loading = true;
      this.circularError = null;
      this.selectedNodeId = null;
      this.graphNotice = '';
      this.pending.clear();
      this.expanded.clear();
      this.summary = null;
      this.experts = [];
      this.signatories = [];
      this.actionItems = [];
      this.destroyGraph();

      const currentVisit = ++this.visit;
      this.api.getCircularRecord(id).subscribe({
        next: (data) => {
          if (currentVisit !== this.visit || this.graphDestroyed) return;
          this.circular = data;
          this.circular.normalizedSource = normalizeSource(this.circular.source);
          this.loading = false;
          this.fetchActionItems(id);
          this.fetchSummary(id);
          this.fetchSignatories(id);
          this.fetchExperts(id);
        },
        error: (err) => {
          if (currentVisit !== this.visit || this.graphDestroyed) return;
          this.loading = false;
          this.circularError = err?.status === 0 ? 'connection' : 'not_found';
        }
      });
    });
  }

  private visit = 0;

  ngOnDestroy(): void {
    this.graphDestroyed = true;
    this.destroyGraph();
  }

  private async createGraph(container: HTMLDivElement): Promise<void> {
    const { default: cytoscapeLib } = await import('cytoscape');

    const root = this.circular;
    if (!root || this.graphDestroyed || this.graphContainer !== container || this.cy) return;

    this.cy = cytoscapeLib({
      container,
      elements: [{
        data: { id: root.id, label: root.full_reference || root.circular_id, title: root.title,
                date: root.issue_date, exchange: root.source, is_root: true },
        classes: 'is-root',
      }],
      style: this.graphStyles,
      layout: { name: 'preset' },
      minZoom: 0.1,
      maxZoom: 3,
      wheelSensitivity: 1.25,
      autounselectify: true,
    });

    this.cy.on('tap', 'node', (event) => {
      const node: cytoscape.NodeSingular = event.target;
      if (!node.hasClass('is-unresolved')) {
        this.zone.run(() => this.expandNode(node));
      }
    });

    this.graphResizeObserver = typeof ResizeObserver === 'undefined'
      ? null
      : new ResizeObserver(() => this.handleGraphResize());
    this.graphResizeObserver?.observe(container);
    window.addEventListener('resize', this.handleGraphResize);

    this.cy.zoom(0.8);
    this.cy.center();
  }

  private destroyGraph(): void {
    this.graphResizeObserver?.disconnect();
    this.graphResizeObserver = null;
    window.removeEventListener('resize', this.handleGraphResize);

    if (this.cy) {
      this.cy.destroy();
      this.cy = null;
    }
  }

  private readonly graphStyles: cytoscape.StylesheetJson = [
    {
      selector: 'core',
      style: {
        'background-color': 'transparent',
        'background-opacity': 0,
      },
    },
    {
      selector: 'node',
      style: {
        shape: 'roundrectangle',
        width: 220,
        height: 112,
        'background-color': '#ffffff',
        'background-opacity': 1,
        'background-image': (node: cytoscape.NodeSingular) => this.getNodeBackgroundImage(node.data('exchange'), !!node.data('is_root')),
        'background-fit': 'none',
        'background-width': (node: cytoscape.NodeSingular) => (node.data('is_root') ? 188 : 188),
        'background-height': (node: cytoscape.NodeSingular) => (node.data('is_root') ? 42 : 20),
        'background-position-x': 16,
        'background-position-y': (node: cytoscape.NodeSingular) => (node.data('is_root') ? 6 : 8),
        'background-repeat': 'no-repeat',
        'background-clip': 'node',
        'border-width': 1,
        'border-color': (node: cytoscape.NodeSingular) => this.getHeaderColor(node.data('exchange')),
        label: (node: cytoscape.NodeSingular) => this.getNodeLabel(node),
        color: '#18181b',
        'font-family': 'Inter, sans-serif',
        'font-size': 11,
        'font-weight': 500,
        'text-wrap': 'wrap',
        'text-max-width': '196px',
        'text-valign': 'center',
        'text-halign': 'center',
        'text-justification': 'center',
        'text-margin-y': (node: cytoscape.NodeSingular) => (node.data('is_root') ? 24 : 16),
        'text-outline-color': '#ffffff',
        'text-outline-width': 3,
        'overlay-opacity': 0,
      },
    },
    {
      selector: 'node.is-root',
      style: {
        'border-color': '#5e6ad2',
        'border-width': 2,
        'z-index': 2,
      },
    },
    {
      selector: 'node.is-unresolved',
      style: {
        'border-style': 'dashed',
        'background-color': '#fafafa',
        color: '#71717a',
      },
    },
    {
      selector: 'edge',
      style: {
        width: 1.5,
        'line-color': (edge: cytoscape.EdgeSingular) => this.getEdgeColor(edge.data('rel_type')),
        'target-arrow-color': (edge: cytoscape.EdgeSingular) => this.getEdgeColor(edge.data('rel_type')),
        'target-arrow-shape': 'triangle',
        'curve-style': 'bezier',
        label: (edge: cytoscape.EdgeSingular) => String(edge.data('label') ?? ''),
        color: '#52525b',
        'font-family': 'Inter, sans-serif',
        'font-size': 10,
        'font-weight': 600,
        'text-rotation': 'autorotate',
        'text-background-color': '#ffffff',
        'text-background-opacity': 1,
        'text-background-padding': '2px',
        'text-outline-color': '#ffffff',
        'text-outline-width': 2,
      },
    },
    {
      selector: 'edge.is-unresolved',
      style: {
        'line-style': 'dashed',
      },
    },
  ];

  private getNodeLabel(node: cytoscape.NodeSingular): string {
    const title = this.truncateTitle(String(node.data('title') ?? '').trim());
    const dateRaw = String(node.data('date') ?? '').trim();
    const date = dateRaw ? this.formatDate(dateRaw) : '';
    const label = String(node.data('label') ?? '').trim();

    return [title, date, label].filter(Boolean).join('\n');
  }

  /**
   * Cap a node title to ~2 lines of text at the node's wrap width
   * (font-size 11, text-max-width 196px). Hides the rest with an ellipsis.
   */
  private truncateTitle(title: string, maxLength = 70): string {
    if (title.length <= maxLength) return title;
    return title.slice(0, maxLength - 1).trimEnd() + '…';
  }

  /**
   * Inline SVG with a colored top band and the exchange name rendered in
   * white uppercase text. Used as a per-node `background-image` so each
   * node visually identifies its source without needing an HTML label.
   */
  private getNodeBackgroundImage(exchange: string | null | undefined, isRoot: boolean): string {
    const color = this.getHeaderColor(exchange);
    const text = this.getExchangeLabel(exchange).toUpperCase();
    const pillY = isRoot ? 22 : 0;
    const svgHeight = isRoot ? 42 : 20;
    const badge = isRoot
      ? `<text x="94" y="14" text-anchor="middle" fill="#5e6ad2" ` +
        `font-family="Inter, -apple-system, sans-serif" font-size="9" font-weight="700" ` +
        `letter-spacing="0.6">CURRENT</text>`
      : '';
    const pill =
      `<rect x="0" y="${pillY}" width="188" height="20" rx="10" ry="10" fill="${color}"/>` +
      `<text x="94" y="${pillY + 14}" text-anchor="middle" fill="#ffffff" ` +
      `font-family="Inter, -apple-system, sans-serif" font-size="10" font-weight="700" ` +
      `letter-spacing="0.6">${text}</text>`;
    const svg =
      `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 188 ${svgHeight}" width="188" height="${svgHeight}">` +
      badge +
      pill +
      `</svg>`;
    return `data:image/svg+xml;utf8,${encodeURIComponent(svg)}`;
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

  private expandNode(node: cytoscape.NodeSingular): void {
    const id = node.id();
    const cy = this.cy;
    this.selectedNodeId = id;
    this.graphNotice = '';
    if (!cy || this.expanded.has(id) || this.pending.has(id)) return;

    this.pending.add(id);
    this.api.getReferenceNeighbors(id).subscribe({
      next: ({ references }) => {
        if (this.cy !== cy) return;
        this.pending.delete(id);
        this.expanded.add(id);
        this.addNeighbors(cy, node, references);
        if (this.selectedNodeId === id && !references.length) {
          this.graphNotice = 'This circular has no references.';
        }
      },
      error: () => {
        if (this.cy !== cy) return;
        this.pending.delete(id);
        if (this.selectedNodeId === id) {
          this.graphNotice = 'Could not load references. Click the node to retry.';
        }
      },
    });
  }

  get loadingNeighbors(): boolean {
    return this.pending.size > 0;
  }

  private addNeighbors(cy: cytoscape.Core, parent: cytoscape.NodeSingular, references: DirectReference[]): void {
    const origin = parent.position();
    const newNodes = new Map<string, DirectReference['neighbor']>();
    for (const { neighbor } of references) {
      const id = neighbor.id ?? 'stub:' + neighbor.label;
      if (cy.getElementById(id).empty()) newNodes.set(id, neighbor);
    }
    // Pick free slots near the parent, including after nodes have been dragged.
    const occupied = cy.nodes().map(node => ({ ...node.position() }));
    let ring = 1;
    let slot = 0;
    cy.batch(() => {
      for (const [id, neighbor] of newNodes) {
        let position: cytoscape.Position;
        do {
          const angle = 2 * Math.PI * slot / (8 * ring);
          position = { x: origin.x + 360 * ring * Math.cos(angle),
                       y: origin.y + 220 * ring * Math.sin(angle) };
          if (++slot === 8 * ring) {
            slot = 0;
            ring++;
          }
        } while (occupied.some(p => Math.abs(p.x - position.x) < 245 && Math.abs(p.y - position.y) < 135));
        occupied.push(position);
        cy.add({
          data: { ...neighbor, id },
          classes: neighbor.id ? '' : 'is-unresolved',
          position,
        });
      }
      for (const ref of references) {
        const id = 'edge:' + ref.id;
        if (cy.getElementById(id).nonempty()) continue;
        cy.add({
          data: { id, source: ref.source, target: ref.target ?? 'stub:' + ref.neighbor.label,
                  label: ref.relationship, rel_type: ref.relationship },
          classes: ref.target ? '' : 'is-unresolved',
        });
      }
    });
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
      case 'SEBI': return '#a855f7';
      case 'NSE':  return '#ef4444';
      case 'NCL':  return '#ef4444';
      case 'AFD':  return '#ef4444';
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

  summaryCopied = false;
  private summaryCopiedResetTimer: ReturnType<typeof setTimeout> | null = null;

  async copySummary(): Promise<void> {
    const text = this.summary ?? '';
    if (!text) return;
    try {
      await navigator.clipboard.writeText(text);
      this.markSummaryCopied();
    } catch {
      // navigator.clipboard is undefined outside a secure context (plain
      // http), so fall back to a detached textarea.
      const scratch = document.createElement('textarea');
      scratch.value = text;
      scratch.style.position = 'fixed';
      scratch.style.opacity = '0';
      document.body.appendChild(scratch);
      scratch.select();
      try {
        document.execCommand('copy');
        this.markSummaryCopied();
      } finally {
        scratch.remove();
      }
    }
  }

  private markSummaryCopied(): void {
    this.summaryCopied = true;
    if (this.summaryCopiedResetTimer) clearTimeout(this.summaryCopiedResetTimer);
    this.summaryCopiedResetTimer = setTimeout(() => {
      this.summaryCopiedResetTimer = null;
      this.summaryCopied = false;
    }, 2000);
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

  openCircular() {
    if (!this.circular?.id) return;

    const url = `/api/circulars/${this.circular.id}/content`;
    console.log('[view-circular] opening', url);
    window.open(url, '_blank');
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

  /** Map backend-provided days_until_deadline to a CSS class for color-coding. */
  getRelativeClass(days: number | null): string {
    if (days === null) return '';
    if (days < 0) return 'overdue';
    if (days <= 7) return 'due-soon';
    return 'future';
  }
}
 