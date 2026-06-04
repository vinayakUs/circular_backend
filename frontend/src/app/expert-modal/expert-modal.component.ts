import { Component, EventEmitter, Output, Input, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { NgxExtendedPdfViewerModule, NgxExtendedPdfViewerService } from 'ngx-extended-pdf-viewer';
import { CircularsApiService, Department } from '../services/circulars-api.service';
import { environment } from 'src/environments/environment';

interface Expert {
  id?: string;
  dept_id: string;
  dept_name?: string;
  title: string;
  text: string;
  highlights: any[];
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
  isLoadingPdf = false;
  isRestoringHighlights = false;
  private isRestoring = false;
  private lastSavedCount = 0;
  pendingSelection = '';
  private annotationsRestored = false;

  constructor(private api: CircularsApiService,private pdfViewerService: NgxExtendedPdfViewerService) {}


saveHighlights(): void {
    if (this.isRestoring || this.isRestoringHighlights) return;
    const annotations = this.pdfViewerService.getSerializedAnnotations();
    if (annotations && annotations.length !== this.lastSavedCount) {
      console.log('Annotation count changed:', annotations.length);
      this.lastSavedCount = annotations.length;
    }
  }
async onPdfLoaded(): Promise<void> {
    this.isLoadingPdf = false;
    this.pdfViewerService.switchAnnotationEdtorMode(9);
}

  ngOnInit(): void {
    this.loadDepartments();
  }
async onEvent(type: string, event: any): Promise<void> {
    console.log(type, event);
    if (type === 'annotationLayerRendered' && !this.annotationsRestored) {
      // Only restore on first page
      if (event.pageNumber === 1) {
        await this.restoreHighlights();
      }
    }
  }

  private async restoreHighlights(): Promise<void> {
    if (this.annotationsRestored || this.isRestoring) return;

    // Wait for experts to load if still loading
    if (this.isLoadingExperts) {
      await new Promise(resolve => setTimeout(resolve, 100));
      if (this.isLoadingExperts) {
        await new Promise(resolve => setTimeout(resolve, 200));
      }
    }

    // Get annotations from experts loaded via API (not localStorage)
    const annotations: any[] = [];
    for (const expert of this.experts) {
      if (expert.highlights?.length) {
        annotations.push(...expert.highlights);
      }
    }

    console.log('Loaded annotations from experts:', JSON.stringify(annotations, null, 2));
    if (!annotations?.length) {
      console.log('No annotations found in experts to restore');
      return;
    }

    this.isRestoringHighlights = true;

    this.isRestoring = true;

    // Give editor layer time to initialize after page render
    await new Promise(resolve => setTimeout(resolve, 500));

    // Batch add all annotations without delays - use Promise.all for parallel restoration
    console.log('Restoring ' + annotations.length + ' annotations in batch...');

    const restorePromises = annotations.map(async (annotation, i) => {
      try {
        console.log('Restoring annotation ' + (i + 1) + ' of ' + annotations.length + ':', annotation);
        await this.pdfViewerService.addEditorAnnotation(annotation);
        console.log('Annotation ' + (i + 1) + ' restore called successfully');
      } catch(e) {
        console.warn('Failed to restore annotation:', e);
      }
    });

    // Wait for all annotations to be added
    await Promise.all(restorePromises);

    // Log final state after all restorations
    const finalAnnotations = this.pdfViewerService.getSerializedAnnotations();
    console.log('Final serialized annotations after restore:', finalAnnotations);

    // Sync expert highlights with actual annotation IDs from PDF
    if (finalAnnotations && finalAnnotations.length > 0) {
      for (let i = 0; i < this.experts.length; i++) {
        const expert = this.experts[i];
        if (expert.highlights?.length) {
          // Map old highlight IDs to new ones by matching annotation content/position
          const updatedHighlights: any[] = [];
          for (const oldHighlight of expert.highlights) {
            // Find matching annotation in finalAnnotations by position (rect)
            const matching = finalAnnotations.find((a: any) => {
              // Match by rect coordinates if available
              if (a.rect && oldHighlight.rect && a.rect.length === oldHighlight.rect.length) {
                // Check first 2 coordinates (top-left) to verify it's same position
                return Math.abs(a.rect[0] - oldHighlight.rect[0]) < 2 &&
                       Math.abs(a.rect[1] - oldHighlight.rect[1]) < 2;
              }
              return false;
            });
            if (matching) {
              updatedHighlights.push(matching);
              console.log('Synced highlight: ' + oldHighlight.id + ' -> ' + matching.id);
            } else {
              console.log('No match found for highlight:', oldHighlight.id);
              // Keep original if no match found
              updatedHighlights.push(oldHighlight);
            }
          }
          if (updatedHighlights.length > 0) {
            expert.highlights = updatedHighlights;
          }
        }
      }
      console.log('Experts after syncing:', JSON.stringify(this.experts.map(e => ({id: e.id, highlightsCount: e.highlights?.length})), null, 2));
    }

    this.isRestoring = false;
    this.isRestoringHighlights = false;
    this.annotationsRestored = true;
    this.lastSavedCount = annotations.length;
  }

  ngOnChanges() {
    this.annotationsRestored = false;
    this.isRestoring = false;
    this.lastSavedCount = 0;
    this.isRestoringHighlights = false;
    if (this.circularId) {
      this.pdfUrl = '';
      this.experts = [];
      this.openDeptDropdownIndex = null;
      this.isLoadingPdf = true;
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
          highlights: (typeof e.highlights === 'string' ? JSON.parse(e.highlights) : (e.highlights || [])).map((h: any) => {
            // Fix quadPoints: if it's an object with string keys, convert back to array
            let quadPoints = h.quadPoints;
            if (quadPoints && typeof quadPoints === 'object' && !Array.isArray(quadPoints)) {
              quadPoints = Object.values(quadPoints).map((v: any) => parseFloat(v) || v);
            }

            // Fix outlines: nested arrays with string values
            let outlines = h.outlines;
            if (outlines && typeof outlines === 'object') {
              outlines = outlines.map((inner: any) =>
                Array.isArray(inner) ? inner.map((v: any) => parseFloat(v) || v) : parseFloat(inner) || inner
              );
            }

            // Fix rect and other number arrays
            let rect = h.rect;
            if (rect && typeof rect === 'object') {
              rect = Array.isArray(rect) ? rect.map((v: any) => parseFloat(v) || v) : Object.values(rect).map((v: any) => parseFloat(v) || v);
            }

            return {
              annotationType: parseInt(h.annotationType) || h.annotationType,
              color: Array.isArray(h.color) ? h.color.map((c: any) => parseInt(c) || c) : h.color,
              opacity: parseFloat(h.opacity) || h.opacity,
              thickness: parseFloat(h.thickness) || h.thickness,
              pageIndex: typeof h.pageIndex === 'string' ? parseInt(h.pageIndex) : h.pageIndex,
              rotation: typeof h.rotation === 'string' ? parseInt(h.rotation) : h.rotation,
              popupRef: h.popupRef || '',
              structTreeParentId: h.structTreeParentId || '',
              isCopy: h.isCopy || false,
              quadPoints,
              outlines,
              rect,
            };
          })
        }));
      },
      error: (err) => {
        this.isLoadingExperts = false;
        console.error('Failed to load experts', err);
      },
      complete: () => {
        this.isLoadingExperts = false;
        // Start loading PDF after experts are ready
        this.pdfUrl = `${environment.apiUrl}/api/circulars/${this.circularId}/content`;
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
          const newExpert: Expert = {
            dept_id: '',
            title: 'Expert ' + (this.experts.length + 1),
            text: highlightText,
            highlights: [] // will be updated after saveHighlights
          };

          this.experts.push(newExpert);
          this.pendingSelection = ''; // clear after use
          console.log('Added highlight to new expert:', newExpert);

          // Now save highlights and get the full annotation to store in expert
          this.saveHighlights();

          // Get the just-added annotation from the PDF viewer to store in expert
          const allAnnotations = this.pdfViewerService.getSerializedAnnotations() || [];
          const justAdded = allAnnotations.find((a: any) => a.id === event.id);
          if (justAdded) {
            newExpert.highlights = [justAdded];
            console.log('Stored full annotation in expert highlights:', justAdded);
          }
          console.log('Current experts list:', this.experts);
        }
      } catch (e) {
        console.warn('Error adding highlight:', e);
      }
    }

    if (['added', 'removed', 'commit'].includes(event.type)) {
      this.saveHighlights();
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
    // Check all tasks have department assigned
    const unassignedTasks = this.experts.filter(e => !e.dept_id);
    if (unassignedTasks.length > 0) {
      this.isSaving = false;
      alert('Please assign a department to all tasks before saving.');
      return;
    }

    this.isSaving = true;

    // Get current annotations from the PDF viewer
    const allAnnotations = this.pdfViewerService.getSerializedAnnotations() || [];

    const payload = {
      experts: this.experts.map(e => {
        // Filter annotations to only those belonging to this expert
        const expertHighlightIds = e.highlights.map(h => h.id);
        const expertAnnotations = allAnnotations.filter((a: any) =>
          expertHighlightIds.includes(a.id)
        );
        const highlightsJson = JSON.stringify(expertAnnotations);

        const expertPayload: any = {
          dept_id: e.dept_id,
          title: e.title,
          text: e.text,
          highlights: highlightsJson
        };
        // Only include id for existing experts (not new ones with undefined id)
        if (e.id) {
          expertPayload.id = e.id;
        }
        return expertPayload;
      }),
      original_ids: this.originalExpertIds
    };
    console.log('Final experts payload:', JSON.stringify(payload, null, 2));
    this.api.saveExperts(this.circularId!, payload.experts as any, this.originalExpertIds).subscribe({
      next: (res) => {
        console.log('Saved successfully:', res);
        this.isSaving = false;
        this.close.emit();
      },
      error: (err) => {
        console.error('Failed to save tasks:', err);
        this.isSaving = false;
        alert('Failed to save tasks. Please try again.');
      }
    });
  }
}