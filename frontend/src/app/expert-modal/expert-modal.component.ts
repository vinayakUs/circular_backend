import { Component, EventEmitter, Output, Input, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { NgxExtendedPdfViewerModule } from 'ngx-extended-pdf-viewer';
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
  showPopup = false;
  popupX = 0;
  popupY = 0;
  pendingSelection: string = '';

  experts: Expert[] = [];
  departments: Department[] = [];
  openDeptDropdownIndex: number | null = null;
  isSaving = false;
  isLoadingExperts = false;

  constructor(private api: CircularsApiService) {}

  ngOnInit(): void {
    this.loadDepartments();
  }

  ngOnChanges() {
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
        this.experts = res.experts.map(e => ({
          id: e.id,
          dept_id: e.dept_id,
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

  onPdfMouseUp(event: MouseEvent): void {
    const selection = window.getSelection();
    if (selection && selection.toString().trim().length > 0) {
      this.pendingSelection = selection.toString().trim();
      this.popupX = event.clientX;
      this.popupY = event.clientY;
      this.showPopup = true;
    } else {
      this.showPopup = false;
    }
  }

  addHighlight(): void {
    if (this.pendingSelection) {
      const highlightDetail: HighlightDetail = {
        id: crypto.randomUUID(),
        color: '#FFFF98',
        page: 1,
        x: 0,
        y: 0,
        width: 0,
        height: 0
      };

      const newExpert: Expert = {
        dept_id: '',
        title: 'Expert ' + (this.experts.length + 1),
        text: this.pendingSelection,
        highlights: [highlightDetail]
      };

      this.experts.push(newExpert);
      console.log('Added highlight to new expert:', newExpert);
    }
    this.showPopup = false;
    this.pendingSelection = '';
    window.getSelection()?.removeAllRanges();
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
    this.experts.splice(expertIndex, 1);
  }

  onAnnotationEvent(event: any): void {
    console.log('PDF Annotation Event:', event);
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
      }))
    };
    console.log('Final experts payload:', JSON.stringify(payload, null, 2));
    this.api.saveExperts(this.circularId!, payload.experts).subscribe({
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