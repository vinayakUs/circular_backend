import { Component, inject } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { NavbarComponent } from "../navbar/navbar.component";
import { NgIf } from '@angular/common';
import { HttpClient } from '@angular/common/http';
import { Router } from '@angular/router';
import { LoginService } from '../services/login.service';

@Component({
  selector: 'app-login',
  standalone: true,
  imports: [NavbarComponent, FormsModule, NgIf],
  templateUrl: './login.component.html',
  styleUrl: './login.component.css'
})
export class LoginComponent {

  private http = inject(HttpClient);
  private loginService = inject(LoginService);
  private router = inject(Router);

  email = '';
  password = '';
  showPassword = false;
  isLoading = false;

  togglePassword(): void {
    this.showPassword = !this.showPassword;
  }

  onSubmit(): void {
    if (!this.email || !this.password) return;
    this.isLoading = true;

    this.loginService.login({ username: this.email, password: this.password }).subscribe({
      next: (response) => {
        console.log('Login successful:', response);
        // Store token in cookie (session cookie, expires when browser closes)
        const expiry = new Date();
        expiry.setHours(expiry.getHours() + 24);
        document.cookie = `access_token=${response.access_token}; path=/; expires=${expiry.toUTCString()}`;
        this.router.navigate(['/']);
      },
      error: (error) => {
        console.error('Login failed:', error);
        alert('Login failed. Please check your credentials and try again.');
        this.isLoading = false;
      }
    });
  }

  
}