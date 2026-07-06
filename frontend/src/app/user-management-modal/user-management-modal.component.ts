import { Component, EventEmitter, Output, inject, OnInit } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { DatePipe } from '@angular/common';
import { UserService, Department, UserRecord } from '../services/user.service';

@Component({
  selector: 'app-user-management-modal',
  standalone: true,
  imports: [FormsModule, DatePipe],
  templateUrl: './user-management-modal.component.html',
  styleUrl: './user-management-modal.component.css'
})
export class UserManagementModalComponent implements OnInit {
  @Output() close = new EventEmitter<void>();
  private userService = inject(UserService);

  departments: Department[] = [];
  selectedDepartment: Department | null = null;
  users: UserRecord[] = [];
  newUserId = '';
  isLoading = false;
  isLoadingUsers = false;
  errorMessage = '';
  successMessage = '';

  ngOnInit(): void {
    this.loadDepartments();
  }

  loadDepartments(): void {
    this.isLoading = true;
    this.userService.getDepartments().subscribe({
      next: (res) => {
        this.departments = res.items.filter(d => !d.archived);
        this.isLoading = false;
        // Auto-select first department if none selected
        if (!this.selectedDepartment && this.departments.length > 0) {
          this.selectDepartment(this.departments[0]);
        }
      },
      error: (err) => {
        console.error('Failed to load departments', err);
        this.isLoading = false;
        this.errorMessage = 'Failed to load departments';
      },
    });
  }

  selectDepartment(dept: Department): void {
    this.selectedDepartment = dept;
    this.loadDepartmentUsers();
  }

  loadDepartmentUsers(): void {
    if (!this.selectedDepartment) return;
    this.isLoadingUsers = true;
    this.userService.getDepartmentUsers(this.selectedDepartment.id).subscribe({
      next: (res) => {
        this.users = res.users;
        this.isLoadingUsers = false;
      },
      error: (err) => {
        console.error('Failed to load users', err);
        this.isLoadingUsers = false;
      },
    });
  }

  onAddUser(): void {
    if (!this.newUserId.trim()) {
      this.errorMessage = 'Please enter a user ID';
      return;
    }
    if (!this.selectedDepartment) {
      this.errorMessage = 'Please select a department first';
      return;
    }
    this.errorMessage = '';
    this.successMessage = '';

    this.userService.addUserToDepartment(this.selectedDepartment.id, this.newUserId.trim()).subscribe({
      next: () => {
        this.successMessage = 'User added!';
        this.newUserId = '';
        this.loadDepartmentUsers();
      },
      error: (err) => {
        console.error('Failed to add user', err);
        if (err.status === 409) {
          this.errorMessage = 'User already exists';
        } else {
          this.errorMessage = 'Failed to add user. Please try again.';
        }
      },
    });
  }

  onRemoveUser(userId: string): void {
    if (!this.selectedDepartment) return;
    this.userService.removeUserFromDepartment(this.selectedDepartment.id, userId).subscribe({
      next: () => {
        this.loadDepartmentUsers();
      },
      error: (err) => {
        console.error('Failed to remove user', err);
        this.errorMessage = 'Failed to remove user';
      },
    });
  }
}
