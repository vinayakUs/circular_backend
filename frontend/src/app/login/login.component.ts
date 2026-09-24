import { Component, OnInit, OnDestroy, inject } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { CommonModule } from '@angular/common';
import { NavbarComponent } from '../navbar/navbar.component';
import { HttpErrorResponse } from '@angular/common/http';
import { Router } from '@angular/router';
import { LoginService } from '../services/login.service';

type LoginStep = 'credentials' | 'mfa' | 'submitting';

@Component({
  selector: 'app-login',
  standalone: true,
  imports: [NavbarComponent, FormsModule, CommonModule],
  templateUrl: './login.component.html',
  styleUrl: './login.component.css'
})
export class LoginComponent implements OnInit, OnDestroy {

  private loginService = inject(LoginService);
  private router = inject(Router);

  step: LoginStep = 'credentials';

  // ── Credentials step ──────────────────────────────────────────────
  username = '';
  password = '';
  captchaAnswer = '';
  challengeId = '';
  captchaImageData = '';
  isLoading = false;
  captchaLoading = false;
  private captchaRequestSeq = 0;

  // ── MFA step ─────────────────────────────────────────────────────
  mfaToken = '';
  availableChannels: string[] = [];
  selectedChannels = new Set<string>();
  otp = '';
  isSendingCode = false;
  isVerifying = false;
  codeSent = false;
  mfaError = '';
  ttlSeconds = 0;
  private countdownHandle: ReturnType<typeof setInterval> | null = null;

  ngOnInit(): void {
    this.refreshCaptcha();
  }

  ngOnDestroy(): void {
    this.stopCountdown();
  }

  // ── Captcha ──────────────────────────────────────────────────────
  refreshCaptcha(): void {
    if (this.captchaLoading) return;
    this.captchaLoading = true;
    this.captchaAnswer = '';
    this.challengeId = '';
    this.captchaImageData = '';

    const mySeq = ++this.captchaRequestSeq;
    this.loginService.fetchCaptcha().subscribe({
      next: (resp) => {
        if (mySeq !== this.captchaRequestSeq) return;
        this.challengeId = resp.challenge_id;
        this.captchaImageData = resp.image_data;
        this.captchaLoading = false;
      },
      error: () => {
        if (mySeq !== this.captchaRequestSeq) return;
        this.captchaLoading = false;
      },
    });
  }

  // ── Step 1: submit credentials + captcha ────────────────────────
  onSubmit(): void {
    if (this.step !== 'credentials') return;
    if (!this.username || !this.password || !this.captchaAnswer || !this.challengeId) return;
    this.isLoading = true;

    this.loginService.login({
      username: this.username,
      password: this.password,
      captcha_id: this.challengeId,
      captcha_answer: this.captchaAnswer,
    }).subscribe({
      next: (resp: any) => {
        this.isLoading = false;

        // MFA path
        if (resp.mfa_required && resp.mfa_token) {
          this.mfaToken = resp.mfa_token;
          this.availableChannels = Array.isArray(resp.channels) ? resp.channels : ['email'];
          this.selectedChannels = new Set<string>();
          if (this.availableChannels.includes('email')) {
            this.selectedChannels.add('email');   // pre-check email by default
          }
          this.step = 'mfa';
          return;
        }

        // Direct JWT path (admin bypass / no-MFA flow)
        this.fetchUserAndGoHome();
      },
      error: (err: HttpErrorResponse) => {
        this.isLoading = false;
        this.refreshCaptcha();
        alert(err?.error?.error || 'Login failed. Please try again.');
      }
    });
  }

  // ── Step 2: MFA — channel selection + code sending ──────────────
  toggleChannel(ch: string): void {
    if (this.selectedChannels.has(ch)) {
      this.selectedChannels.delete(ch);
    } else {
      this.selectedChannels.add(ch);
    }
  }

  isSelected(ch: string): boolean {
    return this.selectedChannels.has(ch);
  }

  channelLabel(ch: string): string {
    return ch === 'email' ? 'Email' : ch === 'sms' ? 'SMS' : ch;
  }

  onSendCode(): void {
    if (this.selectedChannels.size === 0) return;
    this.isSendingCode = true;
    this.mfaError = '';

    this.loginService.mfaRequest(
      this.mfaToken,
      [...this.selectedChannels]
    ).subscribe({
      next: (resp) => {
        this.isSendingCode = false;
        this.codeSent = true;
        this.ttlSeconds = resp.ttl_seconds;
        this.startCountdown();
      },
      error: (err: HttpErrorResponse) => {
        this.isSendingCode = false;
        this.mfaError =
          err?.error?.error || 'Failed to send code. Please try again.';
        // 401 → token expired/invalid → back to login
        if (err?.status === 401) {
          setTimeout(() => this.resetToCredentials(), 1500);
        }
      }
    });
  }

  onResendCode(): void {
    this.stopCountdown();
    this.codeSent = false;
    this.otp = '';
    this.onSendCode();
  }

  // ── Step 3: MFA — verify code ──────────────────────────────────
  onVerifyCode(): void {
    if (this.otp.length !== 6) return;
    this.isVerifying = true;
    this.mfaError = '';

    this.loginService.mfaVerify(this.mfaToken, this.otp).subscribe({
      next: () => {
        this.isVerifying = false;
        this.stopCountdown();
        this.fetchUserAndGoHome();
      },
      error: (err: HttpErrorResponse) => {
        this.isVerifying = false;
        this.otp = '';
        if (err?.status === 423) {
          this.mfaError = 'Too many attempts. Please request a new code.';
          this.codeSent = false;       // show Resend button
        } else if (err?.status === 401) {
          this.mfaError = 'Invalid code. Please try again.';
        } else {
          this.mfaError =
            err?.error?.error || 'Verification failed. Please try again.';
        }
      }
    });
  }

  // ── Helpers ─────────────────────────────────────────────────────
  onBackToCredentials(): void {
    this.stopCountdown();
    this.resetToCredentials();
  }

  private resetToCredentials(): void {
    this.step = 'credentials';
    this.mfaToken = '';
    this.availableChannels = [];
    this.selectedChannels = new Set<string>();
    this.otp = '';
    this.codeSent = false;
    this.mfaError = '';
    this.ttlSeconds = 0;
    this.refreshCaptcha();
  }

  private fetchUserAndGoHome(): void {
    this.loginService.getCurrentUser().subscribe({
      next: (user) => {
        localStorage.setItem('user_details', JSON.stringify(user));
        this.router.navigate(['/']);
      },
      error: () => this.router.navigate(['/']),
    });
  }

  ttlDisplay(): string {
    const m = Math.floor(Math.max(0, this.ttlSeconds) / 60);
    const s = Math.max(0, this.ttlSeconds) % 60;
    return `${m}:${s.toString().padStart(2, '0')}`;
  }

  private startCountdown(): void {
    this.stopCountdown();
    this.countdownHandle = setInterval(() => {
      this.ttlSeconds -= 1;
      if (this.ttlSeconds <= 0) {
        this.stopCountdown();
        this.codeSent = false;
      }
    }, 1000);
  }

  private stopCountdown(): void {
    if (this.countdownHandle !== null) {
      clearInterval(this.countdownHandle);
      this.countdownHandle = null;
    }
  }
}
