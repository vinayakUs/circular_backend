import { Component, ElementRef, HostListener, inject, OnInit, signal, ViewChild } from '@angular/core';
import { ActivatedRoute, Router } from '@angular/router';
import { FormsModule } from '@angular/forms';
import { DatePipe } from '@angular/common';
import { EditorAnnotation, NgxExtendedPdfViewerModule, NgxExtendedPdfViewerService } from 'ngx-extended-pdf-viewer';
import { environment } from 'src/environments/environment';
import { NavbarComponent } from '../navbar/navbar.component';
import { CircularsApiService, Department, Expert } from '../services/circulars-api.service';
import { AnnotationApiService } from './annotation-api.service';
import { AnnotationEntry, Task, TaskService } from './task.service';
import { AnnotationMode } from './annotation-mode.enum';


interface AnnotationEditorEventLike {
  type?: string;
  editorType?: string | number;
  id?: string;
  source?: { name?: string; text?: string; parent?: { id?: string } };
  value?: string | { text?: string; color?: string; thickness?: number; isFreeHighlight?: boolean };
}

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
  selector: 'app-testpdfviewer',
  imports: [NavbarComponent, NgxExtendedPdfViewerModule, FormsModule, DatePipe],
  templateUrl: './testpdfviewer.component.html',
  styleUrl: './testpdfviewer.component.css'
})
export class TestpdfviewerComponent implements OnInit {

  pdfService = inject(NgxExtendedPdfViewerService);
  circularsApi = inject(CircularsApiService);
  taskService = inject(TaskService);
  readonly currentMode = signal<AnnotationMode>(AnnotationMode.NONE);

  /** Run auto-load only once after the editor layer is ready. */
  private autoLoadDone = false;

  /**
   * Deep-link focus (`?expertId=`). Applying it needs BOTH the experts fetched
   * and the pdf.js editor layer mounted, and those two race — whichever
   * finishes last triggers `maybeAutoLoad()`.
   */
  private pendingExpertId: string | null = null;
  private lastAppliedExpertId: string | null = null;
  private expertsLoaded = false;
  private layerReady = false;

  /** Set when `?expertId=` points at an expert that isn't on this circular. */
  expertNotFound = false;

  readonly activeTask = this.taskService.activeTask;
  readonly savedTasks = this.taskService.savedTasks;



  pdfUrl = '';
  circularId = '';
  isAddingTask = false;
  departments: Department[] = [];
  newTaskDeptIds: string[] = [];
  newTaskTitle = '';
  showDeptDropdown = false;

  // ── Comments per saved task ─────────────────────────────────
  selectedTask: Task | null = null;
  comments: Comment[] = [];
  newComment = '';
  isLoadingComments = false;
  isSubmittingComment = false;

  // Inline dept edit (rendered at the bottom of the expert-section)
  isEditingDept = false;
  editedDeptIds: string[] = [];
  showEditDeptDropdown = false;
  isSavingDept = false;

  // Loading flag for the initial experts fetch — keeps the empty state from
  // flashing before the API responds.
  isLoadingExperts = false;

  // ── @mention picker ─────────────────────────────────────────
  @ViewChild('commentBox') commentBox!: ElementRef<HTMLTextAreaElement>;
  mentionOpen = false;
  mentionUsers: { id: string; user_id: string }[] = [];
  mentionDepartments: { id: string; name: string }[] = [];
  mentionActive = 0;
  mentionTriggerPos: number | null = null;
  mentionPos: { top: number; left: number; width: number } | null = null;
  private mentionDebounce?: ReturnType<typeof setTimeout>;




  constructor(
    private route: ActivatedRoute,
    private router: Router
  ) { }

  toggleAddTask(): void {
    this.isAddingTask = !this.isAddingTask;
  }

  cancelAddTask(): void {
    // Remove any highlights captured during this session from the PDF
    const task = this.taskService.activeTask();
    if (task) {
      for (const annotation of task.annotations) {
        try {
          this.pdfService.removeEditorAnnotations((a: any) => a.id === annotation.id);
        } catch (err) {
          console.warn('[cancelAddTask] remove failed', err);
        }
      }
    }

    this.taskService.clearActiveTask();
    this.isAddingTask = false;
    this.newTaskDeptIds = [];
    this.newTaskTitle = '';
    this.showDeptDropdown = false;
    this.setMode(AnnotationMode.NONE);
  }

  ngOnInit(): void {
    this.loadDepartments();

    this.route.queryParamMap.subscribe(params => {
      const circularId = params.get('id');
      if (!circularId) {
        this.router.navigate(['/']);
        return;
      }

      const expertId = params.get('expertId');

      // Same circular, only ?expertId changed — re-focus in place instead of
      // refetching and re-rendering the PDF.
      if (circularId === this.circularId) {
        if (this.autoLoadDone && expertId !== this.lastAppliedExpertId) {
          this.pendingExpertId = expertId;
          this.applyExpertFocus();
        }
        return;
      }

      this.circularId = circularId;
      this.pdfUrl = `${environment.apiUrl}/api/circulars/${circularId}/content`;

      // The task store is root-provided and survives navigation; start clean.
      this.taskService.reset();
      this.selectedTask = null;
      this.comments = [];
      this.expertNotFound = false;
      this.pendingExpertId = expertId;
      this.lastAppliedExpertId = null;
      this.autoLoadDone = false;
      this.expertsLoaded = false;
      this.layerReady = false;

      this.fetchExperts();
    });
  }

  /**
   * Runs once both the experts list and the pdf.js editor layer are ready.
   * With `?expertId=` we jump straight to that expert (its highlights only);
   * otherwise we fall back to rendering every saved highlight.
   */
  private maybeAutoLoad(): void {
    if (this.autoLoadDone || !this.layerReady || !this.expertsLoaded) return;
    this.autoLoadDone = true;

    if (this.pendingExpertId) {
      this.applyExpertFocus();
    } else {
      this.loadFromApi();
    }
  }

  /** Open the expert named by `pendingExpertId`, or show everything if absent. */
  private applyExpertFocus(): void {
    const expertId = this.pendingExpertId;
    this.pendingExpertId = null;
    this.lastAppliedExpertId = expertId;

    if (!expertId) {
      this.expertNotFound = false;
      this.selectedTask = null;
      this.comments = [];
      this.showAllInPdf();
      return;
    }

    const task = this.savedTasks().find(t => t.id === expertId);
    if (!task) {
      console.warn('[applyExpertFocus] expert not on this circular', expertId);
      this.expertNotFound = true;
      this.showAllInPdf();
      return;
    }

    this.expertNotFound = false;
    this.selectTaskForComments(task);
  }

  /** Mirror the current selection into the URL so the view is shareable / reloadable. */
  private syncExpertIdInUrl(expertId: string | null): void {
    this.lastAppliedExpertId = expertId;
    this.router.navigate([], {
      relativeTo: this.route,
      queryParams: { expertId },
      queryParamsHandling: 'merge',
      replaceUrl: true,
    });
  }

  loadDepartments(): void {
    this.circularsApi.getAvailableDepartments().subscribe({
      next: response => {
        this.departments = response.items.filter(department => !department.archived);
      },
      error: error => {
        console.error('Failed to load departments', error);
      }
    });
  }

  toggleNewTaskDepartment(deptId: string): void {
    this.newTaskDeptIds = this.newTaskDeptIds.includes(deptId)
      ? this.newTaskDeptIds.filter(id => id !== deptId)
      : [...this.newTaskDeptIds, deptId];
    this.taskService.setActiveTaskDepartments(this.newTaskDeptIds);
  }

  isNewTaskDeptSelected(deptId: string): boolean {
    return this.newTaskDeptIds.includes(deptId);
  }

  removeNewTaskDept(deptId: string): void {
    this.newTaskDeptIds = this.newTaskDeptIds.filter(id => id !== deptId);
    this.taskService.setActiveTaskDepartments(this.newTaskDeptIds);
  }

  getDeptName(deptId: string): string {
    return this.departments.find(department => department.id === deptId)?.name ?? '';
  }

  onAnnotationEditorModeChanged(event: { mode: number }): void {
    this.currentMode.set(event.mode as AnnotationMode);
  }
  setMode(mode: AnnotationMode): void {
    this.pdfService.switchAnnotationEdtorMode(mode);
  }

  selectTask(): void {
    if (!this.taskService.activeTask()) {
      this.taskService.startNewTask(this.circularId);
      this.newTaskTitle = '';
    }
    this.isAddingTask = true;
    this.setMode(AnnotationMode.HIGHLIGHT);
  }

  renameActiveTask(title: string): void {
    const trimmed = title.trim();
    if (!trimmed) return;
    this.taskService.renameActiveTask(trimmed);
  }
  // saveTask(): void {
  //   if (this.newTaskDeptIds.length === 0) return;

  //   const savedTask = this.taskService.saveActiveTask();
  //   if (!savedTask) return;

  //   this.isAddingTask = false;
  //   this.newTaskDeptIds = [];
  //   this.showDeptDropdown = false;
  //   this.setMode(AnnotationMode.NONE);
  // }

  
    saveTask(): void {
      if (this.newTaskDeptIds.length === 0) return;

      const task = this.taskService.activeTask();          // ← this line was missing
      if (!task || task.annotations.length === 0) return;

      const payload: Expert = {
        title: task.name,
        text: task.annotations[0]?.text ?? '',
        dept_ids: [...this.newTaskDeptIds],
        highlights: task.annotations.map(a => a.serialized),
      };

      this.circularsApi.createExpert(this.circularId, payload).subscribe({
        next: (res) => {
          const savedTask = this.taskService.saveActiveTask();
          if (!savedTask) return;

          // Swap the local temp id for the server-assigned id so subsequent
          // PUT /departments, PATCH /status, etc. use the canonical id.
          this.taskService.updateTaskId(savedTask.id, res.expert.id);

          this.isAddingTask = false;
          this.newTaskDeptIds = [];
          this.showDeptDropdown = false;
          this.setMode(AnnotationMode.NONE);
          this.loadFromApi();
        },
        error: (err) => {
          console.error('[saveTask] createExpert failed', err);
          alert('Failed to save task. Please try again.');
        }
      });
    }

  // ── Comments ────────────────────────────────────────────────
  selectTaskForComments(task: Task): void {
    this.selectedTask = task;
    this.comments = [];
    this.expertNotFound = false;
    if (!task.id) return;
    this.loadComments(task.id);
    this.showTaskInPdf(task.id).then(() => this.scrollToTask(task));
    this.syncExpertIdInUrl(task.id);
  }

  /**
   * Scroll the viewer to the page holding a task's first highlight.
   * Deferred + guarded: pdf.js throws from scrollPagePosIntoView if the target
   * page div hasn't been mounted yet.
   */
  private scrollToTask(task: Task): void {
    const first = task.annotations[0];
    if (!first) return;
    const page = (first.pageIndex ?? 0) + 1;
    setTimeout(() => {
      try {
        this.pdfService.scrollPageIntoView(page, { top: 100 });
      } catch (err) {
        console.warn('[scrollToTask] page not rendered yet', err);
      }
    }, 200);
  }

  deselectTask(): void {
    this.selectedTask = null;
    this.comments = [];
    this.newComment = '';
    this.expertNotFound = false;
    this.showAllInPdf();
    this.syncExpertIdInUrl(null);
  }

  closeTask(): void {
    if (!this.selectedTask?.id) return;
    if (!confirm('Are you sure you want to close this task?')) return;

    const taskId = this.selectedTask.id;
    this.circularsApi.updateExpertStatus(this.circularId, taskId, 'closed').subscribe({
      next: () => {
        this.taskService.updateTaskStatus(taskId, 'closed');
        if (this.selectedTask) {
          this.selectedTask = { ...this.selectedTask, taskStatus: 'closed' };
        }
      },
      error: (err) => {
        console.error('[closeTask] failed', err);
        alert('Failed to close task. Please try again.');
      },
    });
  }

  // ── Inline dept edit ───────────────────────────────────────
  private resetDeptEditState(): void {
    this.isEditingDept = false;
    this.editedDeptIds = [];
    this.showEditDeptDropdown = false;
    this.isSavingDept = false;
  }

  startEditDept(): void {
    if (!this.selectedTask) return;
    this.editedDeptIds = [...this.selectedTask.deptIds];
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
    const task = this.selectedTask;
    if (!task?.id || this.isSavingDept) return;
    if (this.editedDeptIds.length === 0) return;

    const taskId = task.id;
    const newDeptIds = [...this.editedDeptIds];

    this.isSavingDept = true;
    this.circularsApi.setExpertDepartments(this.circularId, taskId, newDeptIds).subscribe({
      next: () => {
        // Clear deptNames so the template falls through to deptIds + getDeptName().
        // deptNames is a snapshot from the initial load and would otherwise keep
        // showing the old dept names after an edit.
        if (this.selectedTask) {
          this.selectedTask = { ...this.selectedTask, deptIds: newDeptIds, deptNames: undefined };
        }
        this.taskService.updateTaskDeptIds(taskId, newDeptIds);
        this.resetDeptEditState();
      },
      error: (err) => {
        console.error('[saveEditDept] failed', err);
        this.isSavingDept = false;
        alert('Failed to save departments. Please try again.');
      },
    });
  }

  loadComments(expertId: string): void {
    this.isLoadingComments = true;
    this.circularsApi.getComments(this.circularId, expertId).subscribe({
      next: (res) => {
        this.comments = res?.comments ?? [];
        this.isLoadingComments = false;
      },
      error: (err) => {
        console.error('[comments] load failed', err);
        this.isLoadingComments = false;
      },
    });
  }

  addComment(): void {
    const text = this.newComment.trim();
    if (!text || !this.selectedTask?.id || this.isSubmittingComment) return;

    this.isSubmittingComment = true;
    this.circularsApi.createComment(this.circularId, this.selectedTask.id, text).subscribe({
      next: (res) => {
        this.comments = [...this.comments, res.comment];
        this.newComment = '';
        this.isSubmittingComment = false;
      },
      error: (err) => {
        console.error('[comments] create failed', err);
        this.isSubmittingComment = false;
      },
    });
  }

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
    COMMENT_MENTION_RE.lastIndex = 0;
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

    const m = before.match(/(?:^|[\s.,;!?()])@((?:dep:)?[^\s@]*)$/i);
    if (!m) {
      this.closeMention();
      return;
    }

    this.mentionTriggerPos = caret - m[1].length - 1;
    this.mentionOpen = true;
    this.updatePopupPos();
    this.scheduleMentionSearch(m[1]);
  }

  /** Recompute popup coords from the textarea's viewport bounding rect. */
  private updatePopupPos(): void {
    const ta = this.commentBox?.nativeElement;
    if (!ta) return;
    const r = ta.getBoundingClientRect();
    const POPUP_MAX_H = 260;
    const ROW_H = 36;
    const GAP = 4;
    const vh = window.innerHeight;
    const total = this.flatMentionCount();

    const popupH = total === 1 ? ROW_H : Math.min(total * ROW_H, POPUP_MAX_H);
    const spaceBelow = vh - r.bottom;
    const placeAbove = total === 1 || (spaceBelow < popupH && r.top > spaceBelow);

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
    this.circularsApi.searchMentions(q, 8).subscribe({
      next: (res) => {
        this.mentionUsers = res.users ?? [];
        this.mentionDepartments = res.departments ?? [];
        const total = this.flatMentionCount();
        this.mentionActive = total === 0 ? 0 : Math.min(this.mentionActive, total - 1);
        this.updatePopupPos();
      },
      error: () => this.closeMention(),
    });
  }

  closeMention(): void {
    this.mentionOpen = false;
    this.mentionTriggerPos = null;
    this.mentionPos = null;
  }

  @HostListener('window:scroll')
  @HostListener('window:resize')
  onWindowScrollOrResize(): void {
    if (this.mentionOpen) this.updatePopupPos();
  }

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

  insertMention(kind: 'user' | 'department', item: { user_id?: string; name?: string }): void {
    const ta = this.commentBox?.nativeElement;
    if (!ta || this.mentionTriggerPos === null) return;
    const token = kind === 'user' ? item.user_id! : `dep:${item.name!}`;
    const caret = ta.selectionStart ?? this.newComment.length;
    const before = this.newComment.slice(0, this.mentionTriggerPos);
    const after = this.newComment.slice(caret);
    this.newComment = `${before}@${token} ${after}`;
    const newCaret = before.length + 1 + token.length + 1;
    setTimeout(() => { ta.focus(); ta.setSelectionRange(newCaret, newCaret); });
    this.closeMention();
  }

  /**
  * Resets the PDF editor mode to NONE. addEditorAnnotation() can flip the
  * editor back to HIGHLIGHT so the user can keep annotating — we override
  * that with multiple passes after a short delay.
  */
  private resetModeAfterReadd(): void {
    // pdf.js queues several async HIGHLIGHT-mode flips after addEditorAnnotation.
    // Schedule multiple NONE passes to catch them all — one synchronous + three
    // delayed retries. Without this the editor sometimes stays in HIGHLIGHT mode
    // and the cursor shows as a highlighter instead of a pointer.
    this.setMode(AnnotationMode.NONE);
    setTimeout(() => this.setMode(AnnotationMode.NONE), 50);
    setTimeout(() => this.setMode(AnnotationMode.NONE), 200);
    setTimeout(() => this.setMode(AnnotationMode.NONE), 500);
  }
  async readdAllAnnotations(): Promise<void> {
    const entries = this.taskService.allEntries();
    console.log('[readdAllAnnotations] re-adding', entries.length, 'annotations');

    for (const entry of entries) {
      try {
        await this.pdfService.addEditorAnnotation(entry.serialized);
      } catch (err) {
        console.warn('[readdAllAnnotations] failed for', entry.id, err);
      }
    }
    console.log('[readdAllAnnotations] done');
  }





  
  async loadFromApi(): Promise<void> {
      console.log('[loadFromApi] rehydrating PDF annotations…');
      // Experts are already loaded via fetchExperts() in ngOnInit.
      // This just re-adds the saved highlights to the PDF editor layer.
      await this.readdAllAnnotations();
      this.resetModeAfterReadd();
      console.log('[loadFromApi] done');
    }

  fetchExperts(): void {
    if (!this.circularId) return;
    console.log('[fetchExperts] fetching…');
    this.isLoadingExperts = true;
    this.circularsApi.getExperts(this.circularId).subscribe({
      next: (res) => {
        for (const e of res.experts ?? []) {
          const createdAtMs = e.created_at
            ? new Date(e.created_at).getTime()
            : Date.now();

          this.taskService.importApiTask({
            id: e.id!,
            name: e.title,
            annotations: e.highlights.map(h => ({
              id: h.id ?? crypto.randomUUID(),
              pageIndex: h.pageIndex ?? 0,
              rect: h.rect ?? [0, 0, 0, 0],
              text: e.text ?? '',
              serialized: h as any,
              createdAt: createdAtMs,
            })),
            circularId: this.circularId,
            createdAt: createdAtMs,
            createdAtIso: e.created_at ?? undefined,
            deptIds: e.dept_ids,
            deptNames: e.dept_names,
            taskStatus: e.status,
            createdByUsername: e.created_by_username ?? undefined,
          });
        }
        this.isLoadingExperts = false;
        console.log('[fetchExperts] done');
        this.expertsLoaded = true;
        this.maybeAutoLoad();
      },
      error: (err) => {
        console.error('[fetchExperts] failed', err);
        this.isLoadingExperts = false;
        // Unblock the gate so the editor layer isn't left waiting forever.
        this.expertsLoaded = true;
        this.maybeAutoLoad();
      },
    });
  }


  async showAllInPdf(): Promise<void> {
    await this.pdfService.removeEditorAnnotations(() => true);
    await this.readdAllAnnotations();
    this.resetModeAfterReadd();
  }



  async showTaskInPdf(taskId: string): Promise<void> {
    const task =
      this.taskService.savedTasks().find((t) => t.id === taskId) ??
      (this.taskService.activeTask()?.id === taskId
        ? this.taskService.activeTask()!
        : null);
    if (!task) {
      console.warn('[showTaskInPdf] task not found', taskId);
      return;
    }

    await this.pdfService.removeEditorAnnotations(() => true);

    for (const entry of task.annotations) {
      try {
        await this.pdfService.addEditorAnnotation(entry.serialized);
      } catch (err) {
        console.warn('[showTaskInPdf] failed to re-add', err);
      }
    }

    this.resetModeAfterReadd();
  }





  onEvent(name: string, event: unknown): void {
    console.log(name, event);

    if (name === 'annotationEditorLayerRendered' && !this.autoLoadDone) {
      // pdfLoaded fires before the editor's deserialize() is wired up;
      // annotationEditorLayerRendered fires after — that's when addEditorAnnotation
      // is safe to call.
      console.log('[onEvent] editor layer ready');
      this.layerReady = true;
      this.maybeAutoLoad();
    }

    if (name === 'annotationEditorEvent') {
      const e = event as AnnotationEditorEventLike;
      console.log('[annotationEditorEvent]', {
        type: e?.type,
        editorType: e?.editorType,
        id: e?.id,
        sourceName: e?.source?.name,
      });

      // capture on commit OR added, regardless of editorType — we'll filter later
      const isCommitOrAdded = e?.type === 'commit' || e?.type === 'added';
      if (isCommitOrAdded && e?.id && this.isAddingTask) {
        this.captureHighlight(e.id, e);
      }


        // pdf.js emits a separate colorChanged event after the user picks a new
        // color in the highlight popup. The original commit was already serialized
        // with the old color, so we patch it now on the matching active annotation.
        if (e?.type === 'colorChanged' && e?.id && this.isAddingTask) {
          const hex = typeof e.value === 'string' ? e.value : null;
          if (hex) {
            const m = hex.match(/^#?([a-f\d]{2})([a-f\d]{2})([a-f\d]{2})$/i);
            if (m) {
              const rgb: [number, number, number] = [
                parseInt(m[1], 16),
                parseInt(m[2], 16),
                parseInt(m[3], 16),
              ];
              this.taskService.updateAnnotationColor(e.id, rgb);
            }
          }
        }


    }
  }






  private async captureHighlight(id: string, e: AnnotationEditorEventLike): Promise<void> {
    console.log('[captureHighlight] called for id', id, 'editorType', e.editorType);

    // Prefer text emitted by the annotation event. The editor can provide it
    // either as value.text, source.text, or a plain string depending on the event.
    const valueText = typeof e.value === 'string' ? e.value : e.value?.text;
    const text = (valueText ?? e.source?.text ?? window.getSelection()?.toString() ?? '').trim();

    // try the service first; if it returns null (race), retry after a microtask
    let ann = this.pdfService.getSerializedAnnotation(id) as
      | (EditorAnnotation & { pageIndex: number; rect: [number, number, number, number] })
      | null
      | undefined;

    if (!ann) {
      await new Promise((r) => setTimeout(r, 0));
      ann = this.pdfService.getSerializedAnnotation(id) as
        | (EditorAnnotation & { pageIndex: number; rect: [number, number, number, number] })
        | null
        | undefined;
    }

    if (!ann) {
      console.warn('[captureHighlight] no annotation found for id', id);
      return;
    }

    if (typeof ann.pageIndex !== 'number' || !ann.rect) {
      console.warn('[captureHighlight] annotation missing pageIndex/rect', ann);
      return;
    }

    // store the FULL serialized annotation so we can re-add even after pdf.js
    // loses track of it (e.g. after clearAndReadd).
    const { id: _ignored, ...serialized } = ann;

    const entry: AnnotationEntry = {
      id,
      pageIndex: ann.pageIndex,
      rect: ann.rect,
      text: text ?? '',
      serialized,
      createdAt: Date.now(),
    };

    this.taskService.addAnnotation(entry);
    console.log('[captureHighlight] added entry', entry);
  }


}
