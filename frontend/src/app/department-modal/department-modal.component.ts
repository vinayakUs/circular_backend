import { Component, EventEmitter, Output, inject, OnInit } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { LoginService } from '../services/login.service';

interface Department {
  id: string;
  name: string;
  archived: boolean;
}

@Component({
  selector: 'app-department-modal',
  standalone: true,
  imports: [FormsModule],
  templateUrl: './department-modal.component.html',
  styleUrl: './department-modal.component.css'
})
export class DepartmentModalComponent implements OnInit {
  @Output() close = new EventEmitter<void>();
  private loginService = inject(LoginService);
  departmentName = '';
  errorMessage = '';
  successMessage = '';
  departments: Department[] = [];
  isLoading = false;

  ngOnInit(): void {
    this.loadDepartments();
  }

  loadDepartments(): void {
    this.isLoading = true;
    this.loginService.getProperties('department').subscribe({
      next: (res) => {
        this.departments = res.items.filter(d => !d.archived);
        this.isLoading = false;
      },
      error: (err) => {
        console.error('Failed to load departments', err);
        this.isLoading = false;
      },
    });
  }

  onAdd(): void{
    if (!this.departmentName.trim()) {
      this.errorMessage = 'Please enter a department name';
      return;
    }
    this.errorMessage = '';
    this.successMessage = '';
    this.loginService.createProperty(this.departmentName.trim(), 'department').subscribe({
      next: () => {
        this.successMessage = 'Added!';
        this.departmentName = '';
        this.loadDepartments();
        // setTimeout(() => this.close.emit(), 800);
      },
      error: (err) => {
        console.error('Failed to add department', err);
        if (err.status === 409) {
          this.errorMessage = 'Department already exists';
        } else {
          this.errorMessage = 'Failed to add department. Please try again.';
        }
      },
    });
  }

}
