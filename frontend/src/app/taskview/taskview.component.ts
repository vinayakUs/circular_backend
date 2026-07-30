import { Component, OnInit, ElementRef, ViewChild, HostListener } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { ActivatedRoute } from '@angular/router';
import { NgxExtendedPdfViewerModule, NgxExtendedPdfViewerService } from 'ngx-extended-pdf-viewer';
import { CircularsApiService, Expert, Department } from '../services/circulars-api.service';
import { environment } from 'src/environments/environment';
import { NavbarComponent } from '../navbar/navbar.component';

interface Comment {
  id: string;
  user_db_id: string;
  author_user_id: string;
  author_name: string | null;
  author_email: string | null;
  text: string;
  created_at: string;
}

interface MentionSegment {
  kind: 'text' | 'user' | 'department';
  value: string;
}

// Mirrors the backend `notifications.mention_parser._MENTION_RE` so the
// rendered pills always match what the server actually parsed.
const COMMENT_MENTION_RE = /(?<![A-Za-z0-9_@])@((?:dep:)?)([A-Za-z0-9_.\-@]+)/g;

@Component({
  selector: 'app-taskview',
  standalone: true,
  imports: [CommonModule, FormsModule, NgxExtendedPdfViewerModule, NavbarComponent],
  templateUrl: './taskview.component.html',
  styleUrl: './taskview.component.css'
})
export class TaskviewComponent implements OnInit {
  pdfUrl: string = '';
  circularId: string = '';
  experts: Expert[] = [];
  departments: Department[] = [];
  selectedExpert: Expert | null = null;
  comments: Comment[] = [];
  newComment: string = '';
  isLoadingComments = false;
  isAddingTask = false;
  isSubmittingComment = false;
  pendingSelection = '';
  pendingHighlight: any = null;
  newTaskTitle = '';
  newTaskDeptIds: string[] = [];
  showDeptDropdown = false;

  // Inline dept edit (rendered at the top of the comments section)
  isEditingDept = false;
  editedDeptIds: string[] = [];
  showEditDeptDropdown = false;
  isSavingDept = false;

  // ── @mention picker ─────────────────────────────────────────
  @ViewChild('commentBox') commentBox!: ElementRef<HTMLTextAreaElement>;
  mentionOpen = false;
  mentionUsers: { id: string; user_id: string }[] = [];
  mentionDepartments: { id: string; name: string }[] = [];
  mentionActive = 0;
  mentionTriggerPos: number | null = null;       // char index of the '@' that opened the picker
  mentionPos: { top: number; left: number; width: number } | null = null;  // fixed-positioned popup coords
  private mentionDebounce?: ReturnType<typeof setTimeout>;

  private annotationsRestored = false;
  private isRestoring = false;
  private isClearingHighlights = false;

  // Sort experts: open tasks first, then closed
  get sortedExperts(): Expert[] {
    return [...this.experts].sort((a, b) => {
      if (a.status === 'closed' && b.status !== 'closed') return 1;
      if (a.status !== 'closed' && b.status === 'closed') return -1;
      return 0;
    });
  }

  constructor(
    private route: ActivatedRoute,
    private api: CircularsApiService,
    private pdfViewerService: NgxExtendedPdfViewerService
  ) {}

  // ── @mention picker helpers ────────────────────────────────
  flatMentionCount(): number {
    return this.mentionUsers.length + this.mentionDepartments.length;
  }

  initials(s: string): string {
    return (s.split('@')[0] || s).slice(0, 2).toUpperCase();
  }

  /** Split a comment into alternating plain-text and mention segments so the
   *  template can render @users and @dep:Departments as highlighted pills. */
  parseCommentSegments(text: string): MentionSegment[] {
    if (!text) return [];
    const out: MentionSegment[] = [];
    COMMENT_MENTION_RE.lastIndex = 0;        // reset stateful regex (g flag)
    let lastIndex = 0;
    for (const m of text.matchAll(COMMENT_MENTION_RE)) {
      if (m.index === undefined) break;
      if (m.index > lastIndex) {
        out.push({ kind: 'text', value: text.slice(lastIndex, m.index) });
      }
      const prefix = m[1] ?? '';
      const ident = m[2] ?? '';
      if (prefix === 'dep:') {
        out.push({ kind: 'department', value: `@${prefix}${ident}` });
      } else {
        out.push({ kind: 'user', value: `@${ident}` });
      }
      lastIndex = m.index + m[0].length;
    }
    if (lastIndex < text.length) {
      out.push({ kind: 'text', value: text.slice(lastIndex) });
    }
    return out;
  }

  /** Called on every keystroke in the comment textarea. Opens / closes / filters the picker. */
  onCommentInput(): void {
    const ta = this.commentBox?.nativeElement;
    if (!ta) return;
    const caret = ta.selectionStart ?? this.newComment.length;
    const before = this.newComment.slice(0, caret);

    // Match an '@' that starts a mention token: must be at text-start or after whitespace/punct,
    // followed by optional 'dep:' and then non-space, non-@ chars up to the caret.
    const m = before.match(/(?:^|[\s.,;!?()])@((?:dep:)?[^\s@]*)$/i);
    if (!m) {
      this.closeMention();
      return;
    }

    this.mentionTriggerPos = caret - m[1].length - 1;     // position of the '@'
    this.mentionOpen = true;
    this.updatePopupPos();
    this.scheduleMentionSearch(m[1]);
  }

  /** Recompute popup coords from the textarea's viewport bounding rect.
   *  Flip rules:
   *    • 1 result   → always above (compact, close to the user's eye line)
   *    • many results → flip above only if it wouldn't fit below
   *  The popup's actual height is clamped to its content (1 row ≈ 32px,
   *  many rows up to POPUP_MAX_H), so for the single-row case we use the
   *  content height when computing the flipped position. */
  private updatePopupPos(): void {
    const ta = this.commentBox?.nativeElement;
    if (!ta) return;
    const r = ta.getBoundingClientRect();
    const POPUP_MAX_H = 260;                              // matches CSS .mention-popup max-height
    const ROW_H       = 36;                               // approx single-row height incl. padding
    const GAP         = 4;
    const vh          = window.innerHeight;
    const total       = this.flatMentionCount();

    // Pick an estimated popup height so the "flipped" position lands cleanly.
    const popupH = total === 1 ? ROW_H : Math.min(total * ROW_H, POPUP_MAX_H);

    const spaceBelow = vh - r.bottom;
    const placeAbove =
      total === 1 ||                                      // 1 result: always above
      (spaceBelow < popupH && r.top > spaceBelow);        // many: flip if no room below

    this.mentionPos = {
      top: placeAbove ? r.top - popupH - GAP : r.bottom + GAP,
      left: r.left,
      width: Math.min(r.width, 320),
    };
  }

  private scheduleMentionSearch(q: string): void {
    clearTimeout(this.mentionDebounce);
    this.mentionDebounce = setTimeout(() => this.runMentionSearch(q), 120);
  }

  private runMentionSearch(q: string): void {
    this.api.searchMentions(q, 8).subscribe({
      next: (res) => {
        this.mentionUsers = res.users ?? [];
        this.mentionDepartments = res.departments ?? [];
        const total = this.flatMentionCount();
        this.mentionActive = total === 0 ? 0 : Math.min(this.mentionActive, total - 1);
        this.updatePopupPos();       // re-evaluate (1-result → above rule now applies)
      },
      error: () => this.closeMention(),
    });
  }

  closeMention(): void {
    this.mentionOpen = false;
    this.mentionTriggerPos = null;
    this.mentionPos = null;
  }

  /** Recompute popup position when the user scrolls or resizes while popup is open. */
  @HostListener('window:scroll')
  @HostListener('window:resize')
  onWindowScrollOrResize(): void {
    if (this.mentionOpen) this.updatePopupPos();
  }

  /** Keyboard navigation while the picker is open. */
  onCommentKey(ev: KeyboardEvent): void {
    if (!this.mentionOpen) return;

    const total = this.flatMentionCount();
    if (ev.key === 'ArrowDown') {
      ev.preventDefault();
      if (total) this.mentionActive = (this.mentionActive + 1) % total;
    } else if (ev.key === 'ArrowUp') {
      ev.preventDefault();
      if (total) this.mentionActive = (this.mentionActive - 1 + total) % total;
    } else if (ev.key === 'Enter' || ev.key === 'Tab') {
      if (!total) return;
      ev.preventDefault();
      if (this.mentionActive < this.mentionUsers.length) {
        this.insertMention('user', this.mentionUsers[this.mentionActive]);
      } else {
        const i = this.mentionActive - this.mentionUsers.length;
        this.insertMention('department', this.mentionDepartments[i]);
      }
    } else if (ev.key === 'Escape') {
      ev.preventDefault();
      this.closeMention();
    }
  }

  /** Insert the chosen mention at the @ trigger position and close the picker. */
  insertMention(kind: 'user' | 'department', item: { user_id?: string; name?: string }): void {
    const ta = this.commentBox?.nativeElement;
    if (!ta || this.mentionTriggerPos === null) return;
    const token = kind === 'user' ? item.user_id! : `dep:${item.name!}`;
    const caret = ta.selectionStart ?? this.newComment.length;
    const before = this.newComment.slice(0, this.mentionTriggerPos);
    const after  = this.newComment.slice(caret);
    this.newComment = `${before}@${token} ${after}`;
    const newCaret = before.length + 1 + token.length + 1;       // 1 for '@', 1 for trailing space
    setTimeout(() => { ta.focus(); ta.setSelectionRange(newCaret, newCaret); });
    this.closeMention();
  }

  ngOnInit(): void {
    this.route.params.subscribe(params => {
      const id = params['id'];
      if (id) {
        this.circularId = id;
        this.pdfUrl = `${environment.apiUrl}/api/circulars/${id}/content`;
        this.loadExperts(id);
        this.loadDepartments();
      }
    });

    // Handle expertId query param to auto-select an expert
    this.route.queryParams.subscribe(queryParams => {
      const expertId = queryParams['expertId'];
      console.log("expertid" , expertId);
      if (expertId) {
        // Experts may not be loaded yet, so select after they load
        const existingId = expertId;
        // const trySelect = () => {
        //   const expert = this.experts.find(e => e.id === existingId);
        //   console.log('Trying to select expert with ID:', existingId, 'Found:', expert);
        //   if (expert) {
        //     this.selectExpert(expert);
        //   }
        // };

        console.log('Experts length:', this.experts.length);

        if (this.experts.length > 0) {
          // trySelect();
          const expert = this.experts.find(e => e.id === existingId);
          console.log('Trying to select expert with ID:', existingId, 'Found:', expert);

        }
        //  else {
        //   // Poll until experts are loaded
        //   const checkExperts = setInterval(() => {
        //     if (this.experts.length > 0) {
        //       clearInterval(checkExperts);
        //       trySelect();
        //     }
        //   }, 100);
        // }
      }
    });
  }

  loadExperts(circularId: string): void {
    this.api.getExperts(circularId).subscribe({
      next: (res) => {
        this.experts = res.experts;
        console.log('Experts:', this.experts);
      },
      error: (err) => {
        console.error('Failed to load experts', err);
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

  /**
   * Programmatically select an expert by ID.
   * This will:
   * 1. Set the selected expert
   * 2. Switch to the expert's comments tab
   * 3. Show only the expert's highlights on the PDF
   * 4. Load the expert's comments
   */
  selectExpertById(expertId: string | undefined): void {
    if (!expertId) return;
    const expert = this.experts.find(e => e.id === expertId);
    if (expert) {
      this.selectExpert(expert);
    } else {
      console.warn(`Expert with ID ${expertId} not found`);
    }
  }

  deselectExpert(): void {
    this.selectedExpert = null;
    this.comments = [];
    this.isAddingTask = false;
    this.resetDeptEditState();
    // Force view mode and keep it there
    this.forceViewMode();
    this.showAllHighlights();
  }

  private forceViewMode(): void {
    // Immediately switch to view mode
    this.pdfViewerService.switchAnnotationEdtorMode(0);
    // Also ensure after a short delay in case addEditorAnnotation re-enables it
    setTimeout(() => {
      this.pdfViewerService.switchAnnotationEdtorMode(0);
    }, 100);
  }

  closeExpert(): void {
    if (!this.selectedExpert?.id) return;
    if (!confirm('Are you sure you want to close this task?')) return;

    const expertId = this.selectedExpert.id;
    const circularId = this.circularId;

    this.api.updateExpertStatus(circularId, expertId, 'closed').subscribe({
      next: () => {
        console.log('Task closed');
        // Update local state
        const expert = this.experts.find(e => e.id === expertId);
        if (expert) {
          expert.status = 'closed';
        }
        this.deselectExpert();
      },
      error: (err) => {
        console.error('Failed to close task:', err);
        alert('Failed to close task. Please try again.');
      }
    });
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
    if (!this.newComment.trim() || !this.selectedExpert || this.isSubmittingComment) return;

    this.isSubmittingComment = true;
    this.api.createComment(this.circularId, this.selectedExpert.id!, this.newComment.trim()).subscribe({
      next: (res) => {
        this.comments.push(res.comment);
        this.newComment = '';
        this.isSubmittingComment = false;
      },
      error: (err) => {
        console.error('Failed to add comment', err);
        this.isSubmittingComment = false;
      }
    });
  }

  toggleAddTask(): void {
    this.isAddingTask = !this.isAddingTask;
    if (this.isAddingTask) {
      // Enable annotation mode for adding highlights
      this.pdfViewerService.switchAnnotationEdtorMode(9);
    } else {
      // Disable annotation mode
      this.pdfViewerService.switchAnnotationEdtorMode(0);
      this.resetPendingState();
    }
  }

  cancelAddTask(): void {
    // Remove the pending highlight annotation from PDF if it exists
    if (this.pendingHighlight?.annotationId) {
      this.pdfViewerService.removeEditorAnnotations((annotation: any) => annotation.id === this.pendingHighlight.annotationId);
    }
    this.isAddingTask = false;
    this.pdfViewerService.switchAnnotationEdtorMode(0);
    this.resetPendingState();
  }

  onPdfMouseUp(_event: MouseEvent): void {
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

    if (event.type === 'added' && this.isAddingTask) {
      try {
        const sourceText = typeof event.source?.text === 'string' ? event.source.text : '';
        const selectionText = window.getSelection()?.toString().trim() || '';
        let valueText = '';
        if (typeof event.value === 'string') {
          valueText = event.value.trim();
        } else if (event.value && typeof (event.value as any).text === 'string') {
          valueText = (event.value as any).text.trim();
        }
        const highlightText = (sourceText || selectionText || valueText || this.pendingSelection || '').trim();

        if (highlightText) {
          // Store pending highlight data - don't add to list yet
          this.pendingHighlight = {
            text: highlightText,
            annotationId: event.id
          };
          this.newTaskTitle = 'New Task ' + (this.experts.length + 1);
          this.pendingSelection = '';
        }
      } catch (e) {
        console.warn('Error adding task:', e);
      }
    }

    if (event.type === 'removed' && !this.isClearingHighlights) {
      const annotationId = event.id;
      const expertIndex = this.experts.findIndex(e =>
        e.highlights?.some((h: any) => h.id === annotationId)
      );
      if (expertIndex !== -1) {
        this.experts.splice(expertIndex, 1);
      }
    }
  }

  onPdfLoaded(): void {
    // Force view mode immediately when PDF loads
    this.pdfViewerService.switchAnnotationEdtorMode(0);
  }

  async onEvent(type: string, event: any): Promise<void> {
    if (type === 'annotationLayerRendered' && !this.annotationsRestored && !this.isRestoring) {
      if (event.pageNumber === 1) {
        await this.restoreHighlights();
        // Ensure view mode after restore
        this.forceViewMode();
      }
    }
  }

  private async restoreHighlights(): Promise<void> {
    if (this.annotationsRestored || this.isRestoring) return;
    if (this.experts.length === 0) return;

    this.isRestoring = true;

    const annotations: any[] = [];
    for (const expert of this.experts) {
      if (expert.highlights?.length) {
        annotations.push(...expert.highlights);
      }
    }

    if (!annotations.length) {
      this.isRestoring = false;
      return;
    }

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
    // Ensure view mode after restoring
    this.forceViewMode();
  }

  private async showHighlightForExpert(expert: Expert): Promise<void> {
    this.annotationsRestored = false;
    this.isRestoring = false;
    this.clearHighlights();

    // Switch to view mode immediately
    this.forceViewMode();

    if (!expert.highlights?.length) return;

    await new Promise(resolve => setTimeout(resolve, 100));

    for (const highlight of expert.highlights) {
      try {
        await this.pdfViewerService.addEditorAnnotation(highlight);
      } catch (e) {
        console.warn('Failed to add highlight:', e);
      }
    }

    const page = (expert.highlights[0].pageIndex ?? 0) + 1;
    console.log('Scrolling to page:', page, 'highlight:', expert.highlights[0]);
    // Scroll to the page with a small delay
    setTimeout(() => {
      this.pdfViewerService.scrollPageIntoView(page, { top: 100 });
      this.forceViewMode();
    }, 200);
    this.annotationsRestored = true;
  }

  private async showAllHighlights(): Promise<void> {
    this.annotationsRestored = false;
    this.isRestoring = false;
    this.clearHighlights();
    await new Promise(resolve => setTimeout(resolve, 500));

    console.log('showAllHighlights called, experts count:', this.experts.length);
    for (const expert of this.experts) {
      console.log('Expert:', expert.title, 'has', expert.highlights?.length ?? 0, 'highlights');
    }

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
    // Ensure view mode after restoring all highlights
    this.forceViewMode();
  }

  private clearHighlights(): void {
    this.isClearingHighlights = true;
    this.pdfViewerService.removeEditorAnnotations(() => true);
    setTimeout(() => { this.isClearingHighlights = false; }, 100);
  }

  toggleNewTaskDepartment(deptId: string): void {
    const idx = this.newTaskDeptIds.indexOf(deptId);
    if (idx === -1) {
      this.newTaskDeptIds = [...this.newTaskDeptIds, deptId];
    } else {
      this.newTaskDeptIds = this.newTaskDeptIds.filter(id => id !== deptId);
    }
  }

  isNewTaskDeptSelected(deptId: string): boolean {
    return this.newTaskDeptIds.includes(deptId);
  }

  removeNewTaskDept(deptId: string): void {
    this.newTaskDeptIds = this.newTaskDeptIds.filter(id => id !== deptId);
  }

  getDeptNames(ids: string[] | undefined | null): string {
    if (!ids || ids.length === 0) return '';
    return ids
      .map(id => this.departments.find(d => d.id === id)?.name ?? '')
      .filter(name => !!name)
      .join(', ');
  }

  saveTask(): void {
    if (!this.pendingHighlight) return;
    if (this.newTaskDeptIds.length === 0) return;

    // Capture values before clearing
    const pendingText = this.pendingHighlight.text;
    const pendingAnnotationId = this.pendingHighlight.annotationId;
    const title = this.newTaskTitle || 'New Task ' + (this.experts.length + 1);
    const deptIds = [...this.newTaskDeptIds];

    // Create the expert object
    const newExpert: Expert = {
      dept_ids: deptIds,
      title: title,
      text: pendingText,
      highlights: []
    } as Expert;

    // Get the annotation and attach to expert
    const allAnnotations = this.pdfViewerService.getSerializedAnnotations() || [];
    const annotation = allAnnotations.find((a: any) => a.id === pendingAnnotationId);
    if (annotation) {
      newExpert.highlights = [annotation];
    }

    // Call API to save first
    const circularId = this.pdfUrl.split('/api/circulars/')[1].split('/content')[0];
    this.api.saveExperts(circularId, [newExpert], []).subscribe({
      next: () => {
        console.log('Task saved');
        // Switch back to view mode after saving
        this.pdfViewerService.switchAnnotationEdtorMode(0);
        // Reset the add-task UI now that the server has persisted.
        this.resetPendingState();
        this.isAddingTask = false;
        // The backend's save endpoint returns only { success: true } without
        // the new task's server-assigned UUID, so we refetch the full list to
        // hydrate each item with its real id (and created_at / creator, etc.)
        // before pushing it into the view. Without this the new task appears
        // in the list but can't be opened because selectExpertById() requires
        // a real id.
        this.api.getExperts(circularId).subscribe({
          next: (response) => {
            this.experts = response.experts ?? [];
          },
          error: (refetchErr) => {
            console.warn('Failed to refresh tasks after save', refetchErr);
          }
        });
      },
      error: (err) => {
        console.error('Failed to save task', err);
        // Switch back to view mode on error too
        this.pdfViewerService.switchAnnotationEdtorMode(0);
        // Reset UI state but don't add to list
        this.resetPendingState();
        this.isAddingTask = false;
        alert('Failed to save task. Please try again.');
      }
    });
  }

  private resetPendingState(): void {
    this.pendingHighlight = null;
    this.newTaskTitle = '';
    this.newTaskDeptIds = [];
    this.pendingSelection = '';
    this.showDeptDropdown = false;
  }

  removeExpert(index: number): void {
    const expert = this.experts[index];
    if (expert.highlights) {
      for (const highlight of expert.highlights) {
        this.pdfViewerService.removeEditorAnnotations((annotation: any) => annotation.id === highlight.id);
      }
    }
    this.experts.splice(index, 1);
  }

  getDeptName(deptId: string): string {
    const dept = this.departments.find(d => d.id === deptId);
    return dept ? dept.name : '';
  }

  // ===== Inline dept edit (comments section) =====

  private resetDeptEditState(): void {
    this.isEditingDept = false;
    this.editedDeptIds = [];
    this.showEditDeptDropdown = false;
    this.isSavingDept = false;
  }

  startEditDept(): void {
    if (!this.selectedExpert) return;
    this.editedDeptIds = [...(this.selectedExpert.dept_ids ?? [])];
    this.isEditingDept = true;
    this.showEditDeptDropdown = false;
  }

  cancelEditDept(): void {
    this.resetDeptEditState();
  }

  toggleEditDept(deptId: string): void {
    this.editedDeptIds = this.editedDeptIds.includes(deptId)
      ? this.editedDeptIds.filter(id => id !== deptId)
      : [...this.editedDeptIds, deptId];
  }

  isEditDeptSelected(deptId: string): boolean {
    return this.editedDeptIds.includes(deptId);
  }

  removeEditDept(deptId: string): void {
    this.editedDeptIds = this.editedDeptIds.filter(id => id !== deptId);
  }

  saveEditDept(): void {
    const expert = this.selectedExpert;
    if (!expert?.id || this.isSavingDept) return;
    if (this.editedDeptIds.length === 0) return;

    const expertId = expert.id;
    const newDeptIds = [...this.editedDeptIds];

    this.isSavingDept = true;
    this.api
      .saveExperts(this.circularId, [{ ...expert, dept_ids: newDeptIds }], [expertId])
      .subscribe({
        next: () => {
          // safe: `expert` was captured from `this.selectedExpert` above
          this.selectedExpert!.dept_ids = newDeptIds;
          const listEntry = this.experts.find(e => e.id === expertId);
          if (listEntry) listEntry.dept_ids = newDeptIds;
          this.resetDeptEditState();
        },
        error: (err) => {
          console.error('Failed to save departments', err);
          this.isSavingDept = false;
          alert('Failed to save departments. Please try again.');
        },
      });
  }
}
