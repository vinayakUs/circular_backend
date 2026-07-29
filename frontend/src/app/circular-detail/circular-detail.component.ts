import { AfterViewInit, ChangeDetectorRef, Component, ElementRef, NgZone, OnDestroy, OnInit, ViewChild } from '@angular/core';
import { CommonModule, DatePipe } from '@angular/common';
import { ActivatedRoute, Router, RouterLink, RouterModule } from '@angular/router';
import { NavbarComponent } from '../navbar/navbar.component';
import { DomSanitizer, SafeHtml } from '@angular/platform-browser';
import { marked } from 'marked';
import type cytoscape from 'cytoscape';
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
  imports: [CommonModule, NavbarComponent, DatePipe, ExpertModalComponent, RouterModule, RouterLink],
  templateUrl: './circular-detail.component.html',
  styleUrl: './circular-detail.component.css'
})
export class CircularDetailComponent implements OnInit, AfterViewInit, OnDestroy {

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

  graphElements: cytoscape.ElementDefinition[] = [];
  graphDepthRows = 0;
  private cy: cytoscape.Core | null = null;

  /** Per-row vertical budget: node height (112) + row gap (80) = 192px.
   *  +40px top/bottom padding keeps the rows visually centred. */
  private readonly ROW_HEIGHT_PX = 192;
  private readonly CONTAINER_VERTICAL_PADDING = 80;
  private readonly BASE_CONTAINER_HEIGHT = 560;

  /** Container height grows with the number of depth rows so a 3-level
   *  graph isn't crammed into the same vertical space as a 1-level one. */
  get graphContainerHeight(): number {
    const rows = Math.max(1, this.graphDepthRows);
    return Math.max(this.BASE_CONTAINER_HEIGHT, rows * this.ROW_HEIGHT_PX + this.CONTAINER_VERTICAL_PADDING);
  }

  get graphContainerMinHeight(): number {
    return this.graphContainerHeight;
  }
  private graphResizeObserver: ResizeObserver | null = null;
  private graphInitialization: Promise<void> | null = null;
  private graphDestroyed = false;

  @ViewChild('cyContainer') private cyContainer?: ElementRef<HTMLDivElement>;

  private readonly handleGraphResize = () => {
    if (!this.graphDestroyed) {
      this.cy?.resize();
    }
  };

  constructor(
    private sanitizer: DomSanitizer,
    private route: ActivatedRoute,
    private api: CircularsApiService,
    private auth: LoginService,
    private cdr: ChangeDetectorRef,
    private zone: NgZone,
    private router: Router
  ) {
    this.isLoggedIn = this.auth.isAuthenticated();
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
      this.referenceGraph = null;
      this.referenceGraphError = false;
      this.referenceGraphLoading = false;
      this.graphElements = [];
      this.graphDepthRows = 0;
      this.summary = null;
      this.experts = [];
      this.signatories = [];
      this.actionItems = [];
      this.destroyGraph();

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
    });
  }

  ngAfterViewInit(): void {
    this.initializeGraph();
  }

  ngOnDestroy(): void {
    this.graphDestroyed = true;
    this.destroyGraph();
  }

  private initializeGraph(): void {
    const container = this.cyContainer?.nativeElement;
    if (
      !container ||
      this.cy ||
      this.graphDestroyed ||
      this.graphElements.length === 0 ||
      this.graphInitialization
    ) {
      return;
    }

    this.graphInitialization = this.createGraph(container)
      .catch((error: unknown) => {
        console.warn('[reference-graph] failed to initialise Cytoscape', error);
      })
      .finally(() => {
        this.graphInitialization = null;
      });
  }

  private async createGraph(container: HTMLDivElement): Promise<void> {
    const { default: cytoscapeLib } = await import('cytoscape');

    if (
      this.graphDestroyed ||
      this.cy ||
      this.graphElements.length === 0 ||
      this.cyContainer?.nativeElement !== container
    ) {
      return;
    }

    this.cy = cytoscapeLib({
      container,
      elements: [],
      style: this.graphStyles,
      layout: { name: 'preset' },
      minZoom: 0.1,
      maxZoom: 3,
      wheelSensitivity: 1.25,
      autounselectify: true,
    });

    this.cy.on('tap', 'node', (event) => {
      const node = event.target;
      const nodeId = node.id();
      console.log('[reference-graph] node tap', { nodeId, classes: node.classes() });
      if (typeof nodeId !== 'string' || nodeId.startsWith('stub:')) {
        console.log('[reference-graph] unresolved stub, ignoring tap');
        return;
      }
      const commands = ['/circular', nodeId];
      console.log('[reference-graph] navigating to', commands.join('/'));
      this.zone.run(() => {
        this.router
          .navigate(commands, { onSameUrlNavigation: 'reload' })
          .then((success) => console.log('[reference-graph] navigation result', success))
          .catch((error) => console.warn('[reference-graph] navigation error', error));
      });
    });

    this.graphResizeObserver = typeof ResizeObserver === 'undefined'
      ? null
      : new ResizeObserver(() => this.handleGraphResize());
    this.graphResizeObserver?.observe(container);
    window.addEventListener('resize', this.handleGraphResize);

    this.renderGraph();
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

  private renderGraph(): void {
    if (!this.cy || this.graphDestroyed) return;

    this.zone.runOutsideAngular(() => {
      const cy = this.cy;
      if (!cy) return;

      cy.batch(() => {
        cy.elements().remove();
        if (this.graphElements.length > 0) {
          cy.add(this.graphElements);
        }
      });

      if (this.graphElements.length === 0) return;

      // The 'preset' layout honours the per-node `position` we computed in
      // toGraphModel() — that's how we get strict depth rows instead of a
      // heuristic tree. We then fit + centre + zoom for the initial view.
      cy.layout({
        name: 'preset',
        fit: true,
        padding: 80,
        animate: false,
      } as cytoscape.LayoutOptions).run();
      cy.resize();
      cy.fit(cy.elements(), 80);
      cy.zoom(0.8);
      cy.center();
    });
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

  private fetchReferenceGraph(circularId: string) {
    this.referenceGraphLoading = true;
    this.referenceGraphError = false;
    this.api.getReferenceGraph(circularId).subscribe({
      next: (graph) => {
        if (this.graphDestroyed) return;
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
        this.cdr.detectChanges();
        if (this.graphElements.length === 0) {
          this.destroyGraph();
          return;
        }
        this.initializeGraph();
        this.renderGraph();
      },
      error: (err) => {
        if (this.graphDestroyed) return;
        this.referenceGraph = null;
        this.graphElements = [];
        this.referenceGraphLoading = false;
        this.referenceGraphError = true;
        this.destroyGraph();
        this.cdr.detectChanges();
        console.warn('[reference-graph] failed to load', err?.status, err?.message);
      }
    });
  }

  /** Convert the API response into Cytoscape nodes and edges.
   *  Nodes are positioned strictly by the API-provided `depth` field —
   *  all depth-0 nodes share a row, all depth-1 nodes share the next, etc.
   *  This produces a clean horizontal hierarchy instead of relying on
   *  a layout engine to infer rank from edge direction. */
  private toGraphModel(): void {
    if (!this.referenceGraph) {
      this.graphElements = [];
      return;
    }

    const stubId = (label: string) => `stub:${label}`;

    // Group node indices by depth so we can layout each row independently.
    // Unknown depths get bucketed into the deepest known row so stubs always
    // sit on a real depth line rather than floating at depth 0.
    const maxKnownDepth = this.referenceGraph.nodes.reduce(
      (acc, n) => Math.max(acc, typeof n.depth === 'number' ? n.depth : 0),
      0,
    );
    const rowIndices = new Map<number, number[]>();
    this.referenceGraph.nodes.forEach((node, index) => {
      const d = typeof node.depth === 'number' ? node.depth : maxKnownDepth;
      const bucket = rowIndices.get(d) ?? [];
      bucket.push(index);
      rowIndices.set(d, bucket);
    });

    // Geometry constants — kept in sync with the node CSS (width 220, height 112).
    const NODE_WIDTH = 220;
    const NODE_HEIGHT = 112;
    const GAP_X = 60;
    const GAP_Y = 80;
    const ROW_PADDING_X = 80;

    // Compute the widest row in nodes so every row can share a common width
    // and stay centered on the same vertical axis.
    const widestRowSize = Math.max(1, ...Array.from(rowIndices.values()).map((row) => row.length));
    const rowPixelWidth = widestRowSize * NODE_WIDTH + (widestRowSize - 1) * GAP_X;
    const rowLeftEdge = -rowPixelWidth / 2 + NODE_WIDTH / 2;

    const positions = new Map<number, { x: number; y: number }>();
    const sortedDepths = Array.from(rowIndices.keys()).sort((a, b) => a - b);
    sortedDepths.forEach((depth) => {
      const row = rowIndices.get(depth) ?? [];
      const rowWidth = row.length * NODE_WIDTH + (row.length - 1) * GAP_X;
      const rowStart = -rowWidth / 2 + NODE_WIDTH / 2;
      row.forEach((nodeIndex, slotInRow) => {
        const x = row.length === widestRowSize
          ? rowLeftEdge + slotInRow * (NODE_WIDTH + GAP_X)
          : rowStart + slotInRow * (NODE_WIDTH + GAP_X);
        const y = depth * (NODE_HEIGHT + GAP_Y);
        positions.set(nodeIndex, { x, y });
      });
    });

    const elements: cytoscape.ElementDefinition[] = this.referenceGraph.nodes.map((node, index) => {
      const id = node.id ?? stubId(node.label);
      const classes = [
        node.is_root ? 'is-root' : '',
        node.unresolved ? 'is-unresolved' : '',
      ].filter(Boolean);

      return {
        group: 'nodes',
        data: {
          id,
          label: node.label,
          title: node.title ?? '',
          date: node.issue_date ?? '',
          exchange: node.exchange ?? null,
          is_root: node.is_root,
          unresolved: node.unresolved,
        },
        position: positions.get(index) ?? { x: 0, y: 0 },
        classes,
      };
    });

    this.referenceGraph.links.forEach((link, index) => {
      const target = link.target ?? stubId(link.target_label ?? 'unknown');
      elements.push({
        group: 'edges',
        data: {
          id: `e:${index}`,
          source: link.source,
          target,
          label: link.label,
          rel_type: link.label,
          unresolved: link.target === null,
        },
        classes: link.target === null ? ['is-unresolved'] : [],
      });
    });

    // Stash the row count so the container can grow vertically for deep graphs.
    this.graphDepthRows = sortedDepths.length;
    this.graphElements = elements;
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
