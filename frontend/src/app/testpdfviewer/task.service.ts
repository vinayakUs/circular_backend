import { Injectable, signal } from '@angular/core';
import type { EditorAnnotation } from 'ngx-extended-pdf-viewer';

export { EditorAnnotation };

export interface AnnotationEntry {
  id: string;                       // current pdf.js id (changes on re-add)
  pageIndex: number;
  rect: [number, number, number, number];
  text: string;
  /** Full serialized data captured at highlight time. Used for re-adding. */
  serialized: EditorAnnotation & { [key: string]: unknown };
  createdAt: number;
}

export interface Task {
  id: string;
  name: string;
  status: 'active' | 'saved';
  annotations: AnnotationEntry[];
  deptIds: string[];
  deptNames?: string[];
  taskStatus?: 'open' | 'closed';
  createdByUsername?: string;
  circularId: string;
  createdAt: number;
  createdAtIso?: string;
  savedAt?: number;
}

@Injectable({ providedIn: 'root' })
export class TaskService {
  readonly activeTask = signal<Task | null>(null);
  private taskCounter = 0;

  readonly savedTasks = signal<Task[]>([]);

  /**
   * Clear the whole store. This service is `providedIn: 'root'`, so its state
   * outlives the component — without a reset, re-entering the viewer (or
   * switching to another circular) would append a second copy of every task.
   */
  reset(): void {
    this.activeTask.set(null);
    this.savedTasks.set([]);
    this.taskCounter = 0;
  }

  startNewTask(circularId = ''): Task {
    // Derive the next task number from the saved-list length so the counter
    // is stable across reloads. Strictly sequential — no number reuse; if a
    // task is deleted, its number is skipped.
    this.taskCounter = this.savedTasks().length + 1;
    const task: Task = {
      id: crypto.randomUUID(),
      name: `Task ${this.taskCounter}`,
      status: 'active',
      annotations: [],
      deptIds: [],
      circularId,
      createdAt: Date.now(),
    };
    this.activeTask.set(task);
    return task;
  }

  /** Bulk-import a task returned from the API as a saved task. */
  importApiTask(task: {
    id: string;
    name: string;
    annotations: AnnotationEntry[];
    deptIds?: string[];
    deptNames?: string[];
    taskStatus?: 'open' | 'closed';
    createdByUsername?: string;
    circularId?: string;
    createdAt: number;
    createdAtIso?: string;
  }): Task {
    const imported: Task = {
      id: task.id,
      name: task.name,
      status: 'saved',
      annotations: task.annotations,
      deptIds: [...(task.deptIds ?? [])],
      deptNames: task.deptNames ? [...task.deptNames] : undefined,
      taskStatus: task.taskStatus,
      createdByUsername: task.createdByUsername,
      circularId: task.circularId ?? '',
      createdAt: task.createdAt,
      createdAtIso: task.createdAtIso,
      savedAt: Date.now(),
    };
    this.savedTasks.update((list) => [...list, imported]);
    return imported;
  }
  setActiveTaskDepartments(deptIds: string[]): void {
    const task = this.activeTask();
    if (!task) return;

    this.activeTask.set({
      ...task,
      deptIds: [...deptIds],
    });
  }

  renameActiveTask(name: string): void {
    const task = this.activeTask();
    if (!task) return;
    this.activeTask.set({ ...task, name });
  }

  clearActiveTask(): void {
    this.activeTask.set(null);
  }
  updateAnnotationColor(id: string, rgb: [number, number, number]): void {
    const task = this.activeTask();
    if (!task) return;
    const annotations = task.annotations.map(a =>
      a.id === id
        ? { ...a, serialized: { ...a.serialized, color: rgb } }
        : a
    );
    this.activeTask.set({ ...task, annotations });
  }

  private hexToRgb(hex: string): [number, number, number] {
    const m = hex.match(/^#?([a-f\d]{2})([a-f\d]{2})([a-f\d]{2})$/i);
    if (!m) return [255, 255, 152];
    return [parseInt(m[1], 16), parseInt(m[2], 16), parseInt(m[3], 16)];
  }

  saveActiveTask(): Task | null {
    const task = this.activeTask();
    if (!task || task.annotations.length === 0 || task.deptIds.length === 0) return null;
    const saved: Task = { ...task, status: 'saved', savedAt: Date.now() };
    this.savedTasks.update((list) => [...list, saved]);
    this.activeTask.set(null);
    return saved;
  }

  /** All annotations across active + saved tasks. */
  allEntries(): AnnotationEntry[] {
    const tasks = [this.activeTask(), ...this.savedTasks()].filter(
      (t): t is Task => !!t
    );
    return tasks.flatMap((t) => t.annotations);
  }
  updateTaskStatus(taskId: string, status: 'open' | 'closed'): void {
    this.savedTasks.update((list) =>
      list.map((t) => (t.id === taskId ? { ...t, taskStatus: status } : t))
    );
  }

  updateTaskDeptIds(taskId: string, deptIds: string[]): void {
    this.savedTasks.update((list) =>
      list.map((t) => (t.id === taskId ? { ...t, deptIds: [...deptIds], deptNames: undefined } : t))
    );
  }

  /** Swap a local temp id for the server-assigned id after createExpert. */
  updateTaskId(oldId: string, newId: string): void {
    this.savedTasks.update((list) =>
      list.map((t) => (t.id === oldId ? { ...t, id: newId } : t))
    );
  }

  addAnnotation(entry: AnnotationEntry): void {
    const task = this.activeTask();
    if (!task) return;

    const existingIndex = task.annotations.findIndex(annotation => annotation.id === entry.id);
    const annotations = existingIndex === -1
      ? [...task.annotations, entry]
      : task.annotations.map((annotation, index) =>
        index === existingIndex
          ? { ...entry, text: entry.text || annotation.text }
          : annotation
      );

    this.activeTask.set({
      ...task,
      annotations,
    });
  }
}