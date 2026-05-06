import { Component, inject } from '@angular/core';
import { RouterLink, RouterLinkActive } from '@angular/router';
import { NgIf } from '@angular/common';
import { LoginService } from '../services/login.service';
import { Router } from '@angular/router';
import { DepartmentModalComponent } from '../department-modal/department-modal.component';

@Component({
  selector: 'app-navbar',
  standalone: true,
  imports: [RouterLink, RouterLinkActive, NgIf,DepartmentModalComponent],
  templateUrl: './navbar.component.html',
  styleUrl: './navbar.component.css'
})
export class NavbarComponent {
  private loginService = inject(LoginService);
  private router = inject(Router);
  dropdownOpen = false;

    showDepartmentModal = false;
    navigateToDepartmentModel(): void {
      this.dropdownOpen = false;
      this.showDepartmentModal = true;
    }
  get isAuthenticated(): boolean {
    return this.loginService.isAuthenticated();
  }

  toggleDropdown(): void {
    this.dropdownOpen = !this.dropdownOpen;
  }    
                                                                                             
        
  logout(): void {
    document.cookie = 'access_token=; path=/; expires=Thu, 01 Jan 1970 00:00:00 GMT';
    this.router.navigate(['/']);
  }
}
