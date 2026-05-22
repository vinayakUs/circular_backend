import { Component, EventEmitter, Output, Input, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { NgxExtendedPdfViewerModule, NgxExtendedPdfViewerService } from 'ngx-extended-pdf-viewer';
import { CircularsApiService, Department } from '../services/circulars-api.service';

interface HighlightDetail {
  id: string;
  color: string;
  page: number;
  x: number;
  y: number;
  width: number;
  height: number;
}

interface Expert {
  id?: string;
  dept_id: string;
  dept_name?: string;
  title: string;
  text: string;
  highlights: HighlightDetail[];
}

@Component({
  selector: 'app-expert-modal',
  standalone: true,
  imports: [CommonModule, FormsModule, NgxExtendedPdfViewerModule],
  templateUrl: './expert-modal.component.html',
  styleUrl: './expert-modal.component.css'
})
export class ExpertModalComponent implements OnInit {
  @Input() circularId: string | undefined = undefined;
  @Output() close = new EventEmitter<void>();

  pdfUrl = '';

  experts: Expert[] = [];
  originalExpertIds: string[] = [];
  departments: Department[] = [];
  openDeptDropdownIndex: number | null = null;
  isSaving = false;
  isLoadingExperts = false;
  private isRestoring = false;
  pendingSelection = '';
  private annotationsRestored = false;

  constructor(private api: CircularsApiService,private pdfViewerService: NgxExtendedPdfViewerService) {}


saveHighlights(): void {
    if (this.isRestoring) return;
    // Just sync current experts to DB - experts are already managed by onAnnotationEvent
    this.syncHighlightsToDb();
  }

  private syncHighlightsToDb(): void {
    if (!this.circularId) return;
    this.api.saveExperts(this.circularId, this.experts, this.originalExpertIds).subscribe({
      next: (res) => console.log('Highlights synced to DB:', res),
      error: (err) => console.error('Failed to sync highlights:', err)
    });
  }
async onPdfLoaded(): Promise<void> {
    // Just enable the editor mode — restore happens in loadExperts via onEvent
    this.pdfViewerService.switchAnnotationEdtorMode(9);
}

  ngOnInit(): void {
    this.loadDepartments();
  }
async onEvent(type: string, event: any): Promise<void> {
    console.log(type, event);
    if (type === 'annotationLayerRendered' && !this.annotationsRestored) {
      // Restore highlights after experts are loaded from DB
      this.restoreHighlights();
    }
  }

  private restoreHighlights(): void {
    if (this.annotationsRestored) return;

    const expertsWithHighlights = this.experts.filter(e => e.highlights?.length > 0);
    if (!expertsWithHighlights.length) return;

    this.annotationsRestored = true;
    this.isRestoring = true;

    // Give editor layer time to initialize after page render
    setTimeout(() => {
      for (const expert of expertsWithHighlights) {
        for (const highlight of expert.highlights) {
          try {
            // Build a minimal annotation from the stored highlight detail
            const annotation = {
              id: highlight.id,
              annotationType: 9, // HighlightEditor
              color: this.hexToRgb(highlight.color),
              thickness: 12,
              opacity: 1,
              pageIndex: (highlight.page || 1) - 1,
              rect: [highlight.x || 0, highlight.y || 0, (highlight.x || 0) + (highlight.width || 100), (highlight.y || 0) + (highlight.height || 20)],
              rotation: 0
            };
            this.pdfViewerService.addEditorAnnotation(annotation as any);
          } catch(e) {
            console.warn('Failed to restore highlight:', e);
          }
        }
      }
      this.isRestoring = false;
    }, 300);
  }

  private hexToRgb(hex: string): [number, number, number] {
    const result = /^#?([a-f\d]{2})([a-f\d]{2})([a-f\d]{2})$/i.exec(hex);
    return result
      ? [parseInt(result[1], 16), parseInt(result[2], 16), parseInt(result[3], 16)]
      : [255, 255, 152];
  }

  private rgbToHex(rgb: number[]): string {
    if (!rgb || rgb.length < 3) return '#FFFF98';
    return '#' + rgb.slice(0, 3).map(x => x.toString(16).padStart(2, '0')).join('');
  }

  ngOnChanges() {
    this.annotationsRestored = false;
    this.isRestoring = false;
    if (this.circularId) {
      this.pdfUrl = `/api/circulars/${this.circularId}/content`;
      this.experts = [];
      this.openDeptDropdownIndex = null;
      this.loadExperts();
    }
  }

  loadExperts(): void {
    if (!this.circularId) return;
    this.isLoadingExperts = true;
    this.api.getExperts(this.circularId).subscribe({
      next: (res) => {
        this.originalExpertIds = res.experts.map((e: any) => e.id).filter((id: string) => id);
        this.experts = res.experts.map((e: any) => ({
          id: e.id,
          dept_id: e.dept_id,
          dept_name: e.dept_name,
          title: e.title,
          text: e.text,
          highlights: e.highlights || []
        }));
      },
      error: (err) => {
        this.isLoadingExperts = false;
        console.error('Failed to load experts', err);
      },
      complete: () => {
        this.isLoadingExperts = false;
      }
    });
  }

  loadDepartments(): void {
    this.api.getAvailableDepartments().subscribe({
      next: (res) => {
        this.departments = res.items.filter(d => !d.archived);
      },
      error: (err) => {
        console.error('Failed to load departments', err);
      }
    });
  }

  onPdfMouseUp(_event: MouseEvent): void {
    const selection = window.getSelection();
    if (selection && selection.toString().trim().length > 0) {
      this.pendingSelection = selection.toString().trim();
    }
  }

  addHighlight(): void {
    // No longer used - highlights are added automatically via onAnnotationEvent
  }

  assignDepartment(expertIndex: number, deptId: string): void {
    const dept = this.departments.find(d => d.id === deptId);
    if (this.experts[expertIndex] && dept) {
      this.experts[expertIndex].dept_id = dept.id;
    }
    this.openDeptDropdownIndex = null;
  }

  getDeptName(deptId: string): string {
    const dept = this.departments.find(d => d.id === deptId);
    return dept ? dept.name : '';
  }

  removeExpert(expertIndex: number): void {
    const expert = this.experts[expertIndex];
    // Remove associated PDF highlight annotations
    if (expert.highlights) {
      for (const highlight of expert.highlights) {
        this.pdfViewerService.removeEditorAnnotations((annotation: any) => annotation.id === highlight.id);
      }
    }
    this.experts.splice(expertIndex, 1);
  }

  onAnnotationEvent(event: any): void {
    console.log('PDF Annotation Event:', event);

    if (event.type === 'added') {
      try {
        // Auto-add to experts list when user creates a highlight
        // Try multiple sources for the highlighted text
        const sourceText = typeof event.source?.text === 'string' ? event.source.text : '';
        const selectionText = window.getSelection()?.toString().trim() || '';
        // event.value can be a string OR an object with a .text property
        let valueText = '';
        if (typeof event.value === 'string') {
          valueText = event.value.trim();
        } else if (event.value && typeof (event.value as any).text === 'string') {
          valueText = (event.value as any).text.trim();
        }
        const pendingText = this.pendingSelection || '';
        const highlightText = (sourceText || selectionText || valueText || pendingText || '').trim();

        console.log('Source text:', sourceText, 'Selection text:', selectionText, 'Value text:', valueText, 'Pending:', pendingText, 'Final:', highlightText);
        console.log('Full event.value:', event.value);

        if (highlightText) {
          const ann = event.source as any;
          const highlightDetail: HighlightDetail = {
            id: event.id || crypto.randomUUID(),
            color: '#FFFF98',
            page: event.page || 1,
            x: ann.x || 0,
            y: ann.y || 0,
            width: ann.width || 0,
            height: ann.height || 0
          };

          const newExpert: Expert = {
            id: crypto.randomUUID(),
            dept_id: '',
            title: 'Expert ' + (this.experts.length + 1),
            text: highlightText,
            highlights: [highlightDetail]
          };

          this.experts.push(newExpert);
          this.pendingSelection = ''; // clear after use
          console.log('Added highlight to new expert:', newExpert);
          console.log('Current experts list:', this.experts);
        }
      } catch (e) {
        console.warn('Error adding highlight:', e);
      }
    }

    if (['added', 'removed', 'commit'].includes(event.type)) {
      // Don't auto-save - only save when user clicks the Save button
    }

    if (event.type === 'removed') {
      // Remove the expert whose highlight matches the removed annotation ID
      const annotationId = event.id;
      const expertIndex = this.experts.findIndex(e =>
        e.highlights.some(h => h.id === annotationId)
      );
      if (expertIndex !== -1) {
        this.experts.splice(expertIndex, 1);
        console.log('Removed expert at index:', expertIndex, 'annotationId:', annotationId);
      }
    }
  }

  navigateToHighlight(index: number): void {
    const expert = this.experts[index];
    if (!expert || !expert.highlights?.length) return;

    const highlight = expert.highlights[0];
    const page = highlight.page || 1;

    // Scroll to the page where the highlight is
    this.pdfViewerService.scrollPageIntoView(page, { top: 100 });
  }

  onSave(): void {
    this.isSaving = true;
    const payload = {
      experts: this.experts.map(e => ({
        id: e.id,
        dept_id: e.dept_id,
        title: e.title,
        text: e.text,
        highlights: e.highlights
      })),
      original_ids: this.originalExpertIds
    };
    console.log('Final experts payload:', JSON.stringify(payload, null, 2));
    this.api.saveExperts(this.circularId!, payload.experts, this.originalExpertIds).subscribe({
      next: (res) => {
        console.log('Saved successfully:', res);
        this.isSaving = false;
        this.close.emit();
      },
      error: (err) => {
        console.error('Failed to save experts:', err);
        this.isSaving = false;
      }
    });
  }
}