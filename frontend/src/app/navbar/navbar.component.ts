import { Component, inject, OnInit } from '@angular/core';
import { Router, RouterLink, RouterLinkActive } from '@angular/router';
import { NgIf } from '@angular/common';
import { LoginService } from '../services/login.service';
import { LogoutService } from '../services/logout.service';
import { DepartmentModalComponent } from '../department-modal/department-modal.component';
import { UserManagementModalComponent } from '../user-management-modal/user-management-modal.component';
import { CurrentPersonaService } from '../services/current-persona.service';
import { personaIconPath } from '../util/persona-icons';

@Component({
  selector: 'app-navbar',
  standalone: true,
  imports: [
    RouterLink,
    RouterLinkActive,
    NgIf,
    DepartmentModalComponent,
    UserManagementModalComponent,
  ],
  templateUrl: './navbar.component.html',
  styleUrl: './navbar.component.css',
})
export class NavbarComponent implements OnInit {
  private loginService = inject(LoginService);
  private logoutSvc = inject(LogoutService);
  private personaSvc = inject(CurrentPersonaService);
  private router = inject(Router);

  dropdownOpen = false;
  currentUsername: string = '';

  ngOnInit(): void {
    this.currentUsername = this.loginService.getUsername();
  }

  showDepartmentModal = false;
  showUserManagementModal = false;

  navigateToDepartmentModel(): void {
    this.dropdownOpen = false;
    this.showDepartmentModal = true;
  }

  navigateToUserManagement(): void {
    this.dropdownOpen = false;
    this.showUserManagementModal = true;
  }

  /**
   * Take the user to the persona picker. Lets them switch which persona
   * they're viewing content as; the picker itself handles persisting the
   * choice via CurrentPersonaService.
   */
  navigateToPersonaSelection(): void {
    this.dropdownOpen = false;
    this.router.navigate(['/personas']);
  }

  get isAuthenticated(): boolean {
    return this.loginService.isAuthenticated();
  }

  get currentPersonaSlug(): string | null {
    return this.personaSvc.slug();
  }
  get currentPersonaName(): string | null {
    return this.personaSvc.persona()?.name ?? null;
  }
  /** SVG path for the active persona's icon — falls back to a shield. */
  get currentPersonaIconPath(): string {
    return personaIconPath(this.personaSvc.persona()?.icon);
  }
  toggleDropdown(): void {
    this.dropdownOpen = !this.dropdownOpen;
  }

  logout(): void {
    this.logoutSvc.logout();
  }
}
