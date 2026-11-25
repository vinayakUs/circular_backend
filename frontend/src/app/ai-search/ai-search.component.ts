import { Component, inject } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { Router } from '@angular/router';
import { DomSanitizer, SafeHtml } from '@angular/platform-browser';
import { marked } from 'marked';

import {
  AiSearchApiService,
  AiSearchPayload,
  AiSearchResultItem,
  AiSummarySource,
} from '../services/ai-search-api.service';

const MONTHS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];

@Component({
  selector: 'app-ai-search',
  imports: [CommonModule, FormsModule],
  templateUrl: './ai-search.component.html',
  styleUrl: './ai-search.component.css',
  standalone: true
})
export class AiSearchComponent {
  private api = inject(AiSearchApiService);
  private router = inject(Router);
  private sanitizer = inject(DomSanitizer);

  query = '';
  loading = false;
  error: string | null = null;
  payload: AiSearchPayload | null = null;

  activeSource = '';
  activeYear = '';
  copied = false;
  private copiedTimer: ReturnType<typeof setTimeout> | null = null;
  private summaryHtmlCache: { text: string; value: SafeHtml } | null = null;

  get understoodAs(): string {
    return this.payload?.response.understood_as || this.payload?.plan.understood_as || '';
  }

  get summaryLabel(): string {
    return this.payload?.response.summary?.label ?? 'AI summary';
  }

  get summaryText(): string {
    return this.payload?.response.summary?.text ?? '';
  }

  get sources(): AiSummarySource[] {
    return this.payload?.response.summary?.sources ?? [];
  }

  get results(): AiSearchResultItem[] {
    return this.payload?.response.results ?? [];
  }

  get message(): string | null {
    return this.payload?.response.message ?? null;
  }

  get sourceChips(): string[] {
    return Object.keys(this.payload?.response.facets?.source ?? {});
  }

  get years(): string[] {
    return Object.keys(this.payload?.response.facets?.year ?? {}).sort().reverse();
  }

  get filteredResults(): AiSearchResultItem[] {
    return this.results.filter(r =>
      (!this.activeSource || r.source === this.activeSource) &&
      (!this.activeYear || r.issue_date?.startsWith(this.activeYear))
    );
  }

  submit(): void {
    const q = this.query.trim();
    if (!q || this.loading) return;
    this.loading = true;
    this.error = null;
    this.payload = null;
    this.copied = false;
    this.activeSource = '';
    this.activeYear = '';
    this.api.search(q).subscribe({
      next: p => {
        this.payload = p;
        this.loading = false;
      },
      error: e => {
        this.error = e?.error?.error
          || (e?.status === 503
            ? 'Search service is temporarily unavailable.'
            : 'Search service encountered an error.');
        this.loading = false;
      },
    });
  }

  get summaryHtml(): SafeHtml {
    const text = this.summaryText;
    if (!text) return '';
    if (this.summaryHtmlCache?.text === text) return this.summaryHtmlCache.value;
    const raw = marked.parse(text, { async: false }) as string;
    const doc = new DOMParser().parseFromString(raw, 'text/html');
    const walker = doc.createTreeWalker(doc.body, NodeFilter.SHOW_TEXT);
    const nodes: Text[] = [];
    while (walker.nextNode()) nodes.push(walker.currentNode as Text);
    for (const node of nodes) {
      const value = node.nodeValue ?? '';
      if (!value.includes('[')) continue;
      const parts = value.split(/(\[\d+\])/g);
      if (parts.length === 1) continue;
      const fragment = doc.createDocumentFragment();
      for (const part of parts) {
        const m = /^\[(\d+)\]$/.exec(part);
        if (!m) {
          fragment.appendChild(doc.createTextNode(part));
          continue;
        }
        const chip = doc.createElement('button');
        chip.type = 'button';
        chip.className = 'cite-chip';
        chip.dataset['n'] = m[1];
        chip.textContent = part;
        fragment.appendChild(chip);
      }
      node.parentNode?.replaceChild(fragment, node);
    }
    // Memoised: a fresh SafeHtml every change-detection cycle would rebuild the
    // cite chips on screen and drop their hover/scroll state.
    const value = this.sanitizer.bypassSecurityTrustHtml(doc.body.innerHTML);
    this.summaryHtmlCache = { text, value };
    return value;
  }

  onSummaryClick(event: MouseEvent): void {
    const chip = (event.target as HTMLElement).closest('.cite-chip') as HTMLElement | null;
    const n = chip?.dataset['n'];
    if (!n) return;
    const row = document.getElementById('src-' + n);
    if (!row) return;
    row.scrollIntoView({ behavior: 'smooth', block: 'center' });
    row.classList.remove('flash');
    void row.offsetWidth;
    row.classList.add('flash');
  }

  async copySummary(): Promise<void> {
    if (!this.summaryText) return;
    try {
      await navigator.clipboard.writeText(this.summaryText);
      this.copied = true;
      if (this.copiedTimer) clearTimeout(this.copiedTimer);
      this.copiedTimer = setTimeout(() => (this.copied = false), 2000);
    } catch {
      // clipboard needs a secure context; leave the button inert
    }
  }

  onCardClick(item: AiSearchResultItem): void {
    this.router.navigate(['/circular', item.id]);
  }

  badgeClass(source: string): string {
    return (source || '').toLowerCase();
  }

  formatDate(iso: string | null | undefined): string {
    if (!iso) return '';
    const [y, m, d] = iso.split('-').map(Number);
    if (!y || !m || !d) return iso;
    return `${d} ${MONTHS[m - 1]} ${y}`;
  }
}
