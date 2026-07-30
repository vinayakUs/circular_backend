import { Component, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { ActivatedRoute } from '@angular/router';
import { NgxExtendedPdfViewerModule, NgxExtendedPdfViewerService } from 'ngx-extended-pdf-viewer';
import { CircularsApiService, Expert, Department } from '../services/circulars-api.service';
import { LoginService } from '../services/login.service';
import { environment } from 'src/environments/environment';

interface Comment {
  id: string;
  user_db_id: string;
  author_user_id: string;
  author_name: string | null;
  author_email: string | null;
  text: string;
  created_at: string;
}

@Component({
  selector: 'app-pdf-viewer-page',
  standalone: true,
  imports: [CommonModule, FormsModule, NgxExtendedPdfViewerModule],
  templateUrl: './pdf-viewer-page.component.html',
  styleUrl: './pdf-viewer-page.component.css'
})
export class PdfViewerPageComponent implements OnInit {
  circularId: string = '';
  pdfUrl: string = '';
  currentUsername: string = '';

  experts: Expert[] = [];
  departments: Department[] = [];
  selectedExpert: Expert | null = null;
  comments: Comment[] = [];
  newComment: string = '';
  isLoadingExperts = false;
  isLoadingComments = false;
  isAddingTask = false;
  pendingSelection = '';
  showDeptDropdown = false;

  private annotationsRestored = false;
  private isRestoring = false;

  constructor(
    private route: ActivatedRoute,
    private api: CircularsApiService,
    private loginService: LoginService,
    private pdfViewerService: NgxExtendedPdfViewerService
  ) {}

  ngOnInit(): void {
    this.currentUsername = this.loginService.getUsername();
    this.route.params.subscribe(params => {
      this.circularId = params['id'];
      if (this.circularId) {
        this.pdfUrl = `${environment.apiUrl}/api/circulars/${this.circularId}/content`;
        this.annotationsRestored = false;
        this.loadExperts();
        this.loadDepartments();
      }
    });
  }

  loadExperts(): void {
    this.isLoadingExperts = true;
    this.api.getExperts(this.circularId).subscribe({
      next: (res) => {
        this.experts = res.experts;
        this.isLoadingExperts = false;
      },
      error: (err) => {
        console.error('Failed to load experts', err);
        this.isLoadingExperts = false;
      }
    });
  }

  loadDepartments(): void {
    this.api.getAvailableDepartments().subscribe({
      next: (res) => {
        this.departments = res.items.filter((d: any) => !d.archived);
      },
      error: (err) => {
        console.error('Failed to load departments', err);
      }
    });
  }

  selectExpert(expert: Expert): void {
    this.selectedExpert = expert;
    this.comments = [];
    this.showHighlightForExpert(expert);
    this.loadComments(expert.id!);
  }

  deselectExpert(): void {
    this.selectedExpert = null;
    this.comments = [];
    this.isAddingTask = false;
    this.showAllHighlights();
  }

  toggleAddTask(): void {
    this.isAddingTask = !this.isAddingTask;
    if (this.isAddingTask) {
      this.selectedExpert = null;
      this.pdfViewerService.switchAnnotationEdtorMode(9);
    } else {
      this.pdfViewerService.switchAnnotationEdtorMode(0);
    }
  }

  loadComments(expertId: string): void {
    this.isLoadingComments = true;
    this.api.getComments(this.circularId, expertId).subscribe({
      next: (res) => {
        this.comments = res.comments;
        this.isLoadingComments = false;
      },
      error: (err) => {
        console.error('Failed to load comments', err);
        this.isLoadingComments = false;
      }
    });
  }

  addComment(): void {
    if (!this.newComment.trim() || !this.selectedExpert) return;

    this.api.createComment(this.circularId, this.selectedExpert.id!, this.newComment.trim()).subscribe({
      next: (res) => {
        this.comments.push(res.comment);
        this.newComment = '';
      },
      error: (err) => {
        console.error('Failed to add comment', err);
      }
    });
  }

  onPdfLoaded(): void {
    // Enable annotation mode if adding task, otherwise keep off
    if (this.isAddingTask) {
      this.pdfViewerService.switchAnnotationEdtorMode(9);
    }
  }

  onPdfMouseUp(event: MouseEvent): void {
    const selection = window.getSelection();
    if (selection && selection.toString().trim().length > 0) {
      this.pendingSelection = selection.toString().trim();
    }
    // Hide any annotation popups
    const popups = document.querySelectorAll('.popup, .popover, [class*="popup"]');
    popups.forEach(p => (p as HTMLElement).style.display = 'none');
  }

  onAnnotationEvent(event: any): void {
    console.log('Annotation event:', event);

    if (event.type === 'added') {
      try {
        const sourceText = typeof event.source?.text === 'string' ? event.source.text : '';
        const selectionText = window.getSelection()?.toString().trim() || '';
        let valueText = '';
        if (typeof event.value === 'string') {
          valueText = event.value.trim();
        } else if (event.value && typeof (event.value as any).text === 'string') {
          valueText = (event.value as any).text.trim();
        }
        const pendingText = this.pendingSelection || '';
        const highlightText = (sourceText || selectionText || valueText || pendingText || '').trim();

        if (highlightText) {
          const newExpert: Expert = {
            dept_ids: [],
            title: 'Task ' + (this.experts.length + 1),
            text: highlightText,
            highlights: []
          };

          this.experts.push(newExpert);
          this.pendingSelection = '';

          // Get the just-added annotation
          const allAnnotations = this.pdfViewerService.getSerializedAnnotations() || [];
          const justAdded = allAnnotations.find((a: any) => a.id === event.id);
          if (justAdded) {
            newExpert.highlights = [justAdded];
          }

          // Switch back to view mode
          this.pdfViewerService.switchAnnotationEdtorMode(0);
          this.isAddingTask = false;

          // Select the new expert
          this.selectExpert(newExpert);
        }
      } catch (e) {
        console.warn('Error adding task:', e);
      }
    }

    if (['added', 'removed', 'commit'].includes(event.type)) {
      this.saveHighlights();
    }

    if (event.type === 'removed') {
      const annotationId = event.id;
      const expertIndex = this.experts.findIndex(e =>
        e.highlights.some((h: any) => h.id === annotationId)
      );
      if (expertIndex !== -1) {
        this.experts.splice(expertIndex, 1);
      }
    }
  }

  saveHighlights(): void {
    if (this.isRestoring) return;
    const annotations = this.pdfViewerService.getSerializedAnnotations();
    console.log('Annotations:', annotations?.length);
  }

  async onEvent(type: string, event: any): Promise<void> {
    if (type === 'annotationLayerRendered' && !this.annotationsRestored) {
      if (event.pageNumber === 1) {
        await this.restoreHighlights();
      }
    }
  }

  private async restoreHighlights(): Promise<void> {
    if (this.annotationsRestored || this.isRestoring) return;

    if (this.isLoadingExperts) {
      await new Promise(resolve => setTimeout(resolve, 100));
      if (this.isLoadingExperts) {
        await new Promise(resolve => setTimeout(resolve, 200));
      }
    }

    const annotations: any[] = [];
    for (const expert of this.experts) {
      if (expert.highlights?.length) {
        annotations.push(...expert.highlights);
      }
    }

    if (!annotations?.length) return;

    this.isRestoring = true;
    await new Promise(resolve => setTimeout(resolve, 500));

    for (const annotation of annotations) {
      try {
        await this.pdfViewerService.addEditorAnnotation(annotation);
      } catch (e) {
        console.warn('Failed to restore annotation:', e);
      }
    }

    this.annotationsRestored = true;
    this.isRestoring = false;
  }

  private async showAllHighlights(): Promise<void> {
    this.annotationsRestored = false;
    this.isRestoring = false;
    this.clearHighlights();
    await new Promise(resolve => setTimeout(resolve, 300));

    for (const expert of this.experts) {
      if (expert.highlights?.length) {
        for (const highlight of expert.highlights) {
          try {
            await this.pdfViewerService.addEditorAnnotation(highlight);
          } catch (e) {
            console.warn('Failed to add highlight:', e);
          }
        }
      }
    }
    this.annotationsRestored = true;
  }

  private async showHighlightForExpert(expert: Expert): Promise<void> {
    this.annotationsRestored = false;
    this.isRestoring = false;
    this.clearHighlights();
    this.pdfViewerService.switchAnnotationEdtorMode(0);

    if (!expert.highlights?.length) return;

    await new Promise(resolve => setTimeout(resolve, 300));

    for (const highlight of expert.highlights) {
      try {
        await this.pdfViewerService.addEditorAnnotation(highlight);
      } catch (e) {
        console.warn('Failed to add highlight:', e);
      }
    }

    const page = expert.highlights[0].page || 1;
    this.pdfViewerService.scrollPageIntoView(page, { top: 100 });
    this.annotationsRestored = true;
  }

  private clearHighlights(): void {
    this.pdfViewerService.removeEditorAnnotations(() => true);
  }

  toggleDepartment(expertIndex: number, deptId: string): void {
    const expert = this.experts[expertIndex];
    if (!expert) return;
    const idx = expert.dept_ids.indexOf(deptId);
    if (idx === -1) {
      expert.dept_ids = [...expert.dept_ids, deptId];
    } else {
      expert.dept_ids = expert.dept_ids.filter(id => id !== deptId);
    }
  }

  removeDepartment(expertIndex: number, deptId: string): void {
    const expert = this.experts[expertIndex];
    if (!expert) return;
    expert.dept_ids = expert.dept_ids.filter(id => id !== deptId);
  }

  isDeptSelected(expertIndex: number, deptId: string): boolean {
    return this.experts[expertIndex]?.dept_ids?.includes(deptId) ?? false;
  }

  getDeptNames(ids: string[] | undefined | null): string {
    if (!ids || ids.length === 0) return '';
    return ids
      .map(id => this.departments.find(d => d.id === id)?.name ?? '')
      .filter(name => !!name)
      .join(', ');
  }

  getDeptName(deptId: string): string {
    const dept = this.departments.find(d => d.id === deptId);
    return dept ? dept.name : '';
  }

  removeExpert(index: number): void {
    const expert = this.experts[index];
    if (expert.highlights) {
      for (const highlight of expert.highlights) {
        this.pdfViewerService.removeEditorAnnotations((annotation: any) => annotation.id === highlight.id);
      }
    }
    this.experts.splice(index, 1);
    if (this.selectedExpert === expert) {
      this.selectedExpert = null;
    }
  }
}
