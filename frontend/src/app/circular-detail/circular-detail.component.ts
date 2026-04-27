import { Component, AfterViewInit, OnDestroy, ViewChild, ElementRef } from '@angular/core';
import { CommonModule } from '@angular/common';
import { NavbarComponent } from '../navbar/navbar.component';
import { DomSanitizer, SafeHtml } from '@angular/platform-browser';

const NODE_W = 160;
const NODE_H = 64;

interface GraphNode {
  id: string; label: string; exchange: string; title: string; x: number; y: number; current?: boolean;
}

@Component({
  selector: 'app-circular-detail',
  standalone: true,
  imports: [CommonModule, NavbarComponent],
  templateUrl: './circular-detail.component.html',
  styleUrl: './circular-detail.component.css'
})
export class CircularDetailComponent implements AfterViewInit, OnDestroy {
  @ViewChild('graphContainer') containerRef!: ElementRef<HTMLDivElement>;

  nodes: GraphNode[] = [
    { id: '1', label: 'SEBI/MRD/2025/089', exchange: 'sebi', title: 'Master Circular on AIF', x: 40, y: 20 },
    { id: '4', label: 'NSE/MF/2026/10',    exchange: 'nse',  title: 'Related Compliance',    x: 280, y: 20 },
    { id: '2', label: 'SEBI/MRD/2025/120', exchange: 'sebi', title: 'AIF Guidelines',         x: 40, y: 160 },
    { id: '3', label: 'NMF73772',          exchange: 'nse',  title: 'Maintenance Notice',     x: 160, y: 300, current: true },
    { id: '5', label: 'NSE/MF/2026/15',    exchange: 'nse',  title: 'System Guidelines',      x: 40, y: 440 },
    { id: '6', label: 'NSE/MF/2026/18',    exchange: 'nse',  title: 'Old Notice',             x: 280, y: 440 },
  ] as GraphNode[];

  links = [
    { source: '1', target: '2', label: 'Amends' },
    { source: '2', target: '3', label: 'Extends' },
    { source: '4', target: '3', label: 'Extends' },
    { source: '3', target: '5', label: 'Supersedes' },
    { source: '3', target: '6', label: 'Supersedes' },
  ];

  graphSvg: SafeHtml = '';

  private svgEl!: SVGSVGElement;
  private vbX = 0; private vbY = 0; private vbW = 480; private vbH = 550;
  private isPanning = false;
  private panStart = { x: 0, y: 0, vbX: 0, vbY: 0 };
  private dragging: { el: SVGGElement; data: GraphNode } | null = null;
  private dragOffset = { x: 0, y: 0 };

  constructor(private sanitizer: DomSanitizer) {
    this.graphSvg = this.generateGraphSvg();
  }

  private boundMove = (e: MouseEvent) => this.onMove(e);
  private boundEnd = () => this.endInteraction();

  ngAfterViewInit() {
    this.svgEl = this.containerRef.nativeElement.querySelector('svg')!;
    if (!this.svgEl) return;

    this.svgEl.querySelectorAll('[data-node-id]').forEach(g => {
      g.addEventListener('mousedown', e => this.startNodeDrag(e as MouseEvent, g as SVGGElement));
    });

    this.svgEl.addEventListener('mousedown', e => this.startPan(e as MouseEvent));
    window.addEventListener('mousemove', this.boundMove);
    window.addEventListener('mouseup', this.boundEnd);
    this.svgEl.addEventListener('wheel', e => this.onZoom(e as WheelEvent), { passive: false });
  }

  ngOnDestroy() {
    window.removeEventListener('mousemove', this.boundMove);
    window.removeEventListener('mouseup', this.boundEnd);
  }

  private screenToSvg(clientX: number, clientY: number) {
    const rect = this.svgEl.getBoundingClientRect();
    return {
      x: this.vbX + (clientX - rect.left) * (this.vbW / rect.width),
      y: this.vbY + (clientY - rect.top) * (this.vbH / rect.height),
    };
  }

  private startNodeDrag(e: MouseEvent, g: SVGGElement) {
    e.stopPropagation();
    e.preventDefault();
    const nodeId = g.getAttribute('data-node-id')!;
    const data = this.nodes.find(n => n.id === nodeId)!;
    this.dragging = { el: g, data };
    const pt = this.screenToSvg(e.clientX, e.clientY);
    this.dragOffset = { x: pt.x - data.x, y: pt.y - data.y };
    g.style.cursor = 'grabbing';
  }

  private startPan(e: MouseEvent) {
    if (this.dragging) return;
    this.isPanning = true;
    this.panStart = { x: e.clientX, y: e.clientY, vbX: this.vbX, vbY: this.vbY };
    this.svgEl.style.cursor = 'grabbing';
  }

  private onMove(e: MouseEvent) {
    if (this.dragging) {
      const pt = this.screenToSvg(e.clientX, e.clientY);
      this.dragging.data.x = pt.x - this.dragOffset.x;
      this.dragging.data.y = pt.y - this.dragOffset.y;
      this.dragging.el.setAttribute('transform', `translate(${this.dragging.data.x}, ${this.dragging.data.y})`);
      this.updateLinkPaths();
    } else if (this.isPanning) {
      const rect = this.svgEl.getBoundingClientRect();
      this.vbX = this.panStart.vbX - (e.clientX - this.panStart.x) * (this.vbW / rect.width);
      this.vbY = this.panStart.vbY - (e.clientY - this.panStart.y) * (this.vbH / rect.height);
      this.svgEl.setAttribute('viewBox', `${this.vbX} ${this.vbY} ${this.vbW} ${this.vbH}`);
    }
  }

  private endInteraction() {
    if (this.dragging) {
      this.dragging.el.style.cursor = 'grab';
      this.dragging = null;
    }
    if (this.isPanning) {
      this.svgEl.style.cursor = 'grab';
      this.isPanning = false;
    }
  }

  private onZoom(e: WheelEvent) {
    e.preventDefault();
    const factor = e.deltaY > 0 ? 1.12 : 0.9;
    const newW = Math.min(Math.max(this.vbW * factor, 400), 2400);
    const newH = Math.min(Math.max(this.vbH * factor, 200), 1200);
    const pt = this.screenToSvg(e.clientX, e.clientY);
    this.vbX = pt.x - (pt.x - this.vbX) * (newW / this.vbW);
    this.vbY = pt.y - (pt.y - this.vbY) * (newH / this.vbH);
    this.vbW = newW;
    this.vbH = newH;
    this.svgEl.setAttribute('viewBox', `${this.vbX} ${this.vbY} ${this.vbW} ${this.vbH}`);
  }

  private updateLinkPaths() {
    this.links.forEach(link => {
      const pathEl = this.svgEl.querySelector(`[data-link-id="${link.source}-${link.target}"]`);
      const textEl = this.svgEl.querySelector(`[data-label-id="${link.source}-${link.target}"]`);
      const bgEl = this.svgEl.querySelector(`[data-label-bg="${link.source}-${link.target}"]`);
      if (!pathEl) return;

      const { pathD, lx, ly } = this.computeLink(link);
      pathEl.setAttribute('d', pathD);
      if (textEl) { textEl.setAttribute('x', String(lx)); textEl.setAttribute('y', String(ly + 3)); }
      if (bgEl) { bgEl.setAttribute('x', String(lx - 28)); bgEl.setAttribute('y', String(ly - 10)); }
    });
  }

  private computeLink(link: { source: string; target: string }) {
    const source = this.nodes.find(n => n.id === link.source) as GraphNode;
    const target = this.nodes.find(n => n.id === link.target) as GraphNode;
    const sx = source.x + NODE_W / 2;
    const sy = source.y + NODE_H;
    const tx = target.x + NODE_W / 2;
    const ty = target.y;
    const midY = (sy + ty) / 2;
    const pathD = `M ${sx} ${sy} C ${sx} ${midY}, ${tx} ${midY}, ${tx} ${ty}`;
    const lx = (sx + tx) / 2;
    const ly = midY - 10;
    return { pathD, lx, ly };
  }

  getNodeById(id: string) {
    return this.nodes.find(n => n.id === id);
  }

  getShortId(label: string): string {
    const parts = label.split('/');
    if (parts.length >= 4) {
      return `${parts[0].charAt(0)}/${parts[1].charAt(0)}/${parts[2].slice(-2)}/${parts[3]}`;
    }
    return label;
  }

  generateGraphSvg(): SafeHtml {
    const defs = `
      <defs>
        <marker id="arr" markerWidth="10" markerHeight="7" refX="9" refY="3.5" orient="auto">
          <polygon points="0 0, 10 3.5, 0 7" fill="#818cf8"/>
        </marker>
        <filter id="shadow" x="-20%" y="-20%" width="140%" height="140%">
          <feDropShadow dx="0" dy="2" stdDeviation="4" flood-color="#000" flood-opacity="0.08"/>
        </filter>
        <filter id="glow" x="-20%" y="-20%" width="140%" height="140%">
          <feDropShadow dx="0" dy="0" stdDeviation="6" flood-color="#5e6ad2" flood-opacity="0.4"/>
        </filter>
      </defs>`;

    const linksHtml = this.links.map(link => {
      const { pathD, lx, ly } = this.computeLink(link);
      return `
        <path data-link-id="${link.source}-${link.target}" d="${pathD}"
          stroke="#c7d2fe" stroke-width="2" fill="none" marker-end="url(#arr)" opacity="0.9"/>
        <rect data-label-bg="${link.source}-${link.target}"
          x="${lx - 28}" y="${ly - 10}" width="56" height="18" rx="4"
          fill="#eef2ff" stroke="#c7d2fe" stroke-width="1"/>
        <text data-label-id="${link.source}-${link.target}"
          x="${lx}" y="${ly + 3}"
          font-family="Inter,sans-serif" font-size="10" font-weight="600"
          fill="#4338ca" text-anchor="middle">${link.label}</text>`;
    }).join('');

    const nodesHtml = this.nodes.map(node => {
      const shortId = this.getShortId(node.label);
      const isSebi = node.exchange === 'sebi';
      const accentColor = isSebi ? '#a855f7' : '#ef4444';
      const accentBorder = isSebi ? 'rgba(168,85,247,0.3)' : 'rgba(239,68,68,0.3)';
      const exchangeLabel = isSebi ? 'SEBI' : 'NSE';
      const exchangeBg = isSebi ? 'rgba(168,85,247,0.15)' : 'rgba(239,68,68,0.12)';
      const titleText = node.title.length > 20 ? node.title.substring(0, 20) + '…' : node.title;

      if (node.current) {
        return `
          <g data-node-id="${node.id}" transform="translate(${node.x}, ${node.y})" style="cursor:grab">
            <rect width="${NODE_W}" height="${NODE_H}" rx="10"
              fill="#5e6ad2" stroke="#4f5ac4" stroke-width="1.5" filter="url(#glow)"/>
            <rect x="0" y="0" width="5" height="${NODE_H}" rx="3" fill="#a5b4fc"/>
            <rect x="${NODE_W - 54}" y="10" width="46" height="16" rx="6" fill="rgba(255,255,255,0.2)"/>
            <text x="${NODE_W - 31}" y="22" font-family="Inter,sans-serif" font-size="9" font-weight="700"
              fill="#fff" text-anchor="middle">CURRENT</text>
            <text x="14" y="28" font-family="Inter,sans-serif" font-size="13" font-weight="700" fill="#ffffff">${shortId}</text>
            <text x="14" y="47" font-family="Inter,sans-serif" font-size="10" fill="rgba(255,255,255,0.75)">${titleText}</text>
          </g>`;
      }

      return `
        <g data-node-id="${node.id}" transform="translate(${node.x}, ${node.y})" style="cursor:grab">
          <rect width="${NODE_W}" height="${NODE_H}" rx="10"
            fill="#ffffff" stroke="${accentBorder}" stroke-width="1.5" filter="url(#shadow)"/>
          <rect x="0" y="0" width="5" height="${NODE_H}" rx="3" fill="${accentColor}"/>
          <rect x="12" y="10" width="34" height="15" rx="5" fill="${exchangeBg}"/>
          <text x="29" y="21" font-family="Inter,sans-serif" font-size="9" font-weight="700"
            fill="${accentColor}" text-anchor="middle">${exchangeLabel}</text>
          <text x="54" y="23" font-family="Inter,sans-serif" font-size="12" font-weight="600" fill="#1e1e2e">${shortId}</text>
          <text x="54" y="42" font-family="Inter,sans-serif" font-size="10" fill="#6b7280">${titleText}</text>
        </g>`;
    }).join('');

    const svg = `
      <svg xmlns="http://www.w3.org/2000/svg"
        style="width:100%;height:550px;display:block;cursor:grab"
        viewBox="0 0 480 550">
        <rect width="480" height="550" rx="12" fill="#f8f9ff"/>
        ${defs}
        <g>${linksHtml}</g>
        <g>${nodesHtml}</g>
      </svg>`;

    return this.sanitizer.bypassSecurityTrustHtml(svg);
  }
}
