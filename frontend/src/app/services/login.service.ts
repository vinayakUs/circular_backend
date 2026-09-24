import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Observable } from 'rxjs';
import { environment } from 'src/environments/environment';

@Injectable({
  providedIn: 'root',
})
export class LoginService {
  private baseUrl = environment.apiUrl;

  constructor(private http: HttpClient) {}

  /** Fetch a fresh CAPTCHA challenge.
   *
   * Returns the challenge UUID plus the rendered PNG as a data URL —
   * the <img src="..."> consumes the data URL directly, no second request.
   * Plaintext answer is never in the response.
   */
  fetchCaptcha(): Observable<{ challenge_id: string; image_data: string }> {
    return this.http.get<{ challenge_id: string; image_data: string }>(
      `${this.baseUrl}/api/auth/captcha/new`,
    );
  }

  /**
   * Login. Returns one of two response shapes:
   *   - { mfa_required: true, mfa_token, channels, otp_length, ttl_seconds } — MFA flow
   *   - { access_token, token_type }                                      — direct JWT (admin bypass)
   *
   * Caller checks for `mfa_required` and routes accordingly.
   */
  login(loginData: {
    username: string;
    password: string;
    captcha_id: string;
    captcha_answer: string;
  }): Observable<LoginResponse> {
    return this.http.post<LoginResponse>(
      `${this.baseUrl}/api/auth/login`,
      loginData,
    );
  }

  /**
   * Request an OTP be sent to one or more channels.
   * Server generates ONE code and sends it (same code) to every channel
   * in the list that the user has configured.
   */
  mfaRequest(mfaToken: string, channels: string[]): Observable<MfaRequestResponse> {
    return this.http.post<MfaRequestResponse>(
      `${this.baseUrl}/api/auth/mfa/request`,
      { mfa_token: mfaToken, channels },
    );
  }

  /**
   * Verify the OTP code. On success, server sets the access_token
   * HttpOnly cookie AND returns the token in the body for the SPA.
   */
  mfaVerify(mfaToken: string, otp: string): Observable<{ access_token: string; token_type: string }> {
    return this.http.post<{ access_token: string; token_type: string }>(
      `${this.baseUrl}/api/auth/mfa/verify`,
      { mfa_token: mfaToken, otp },
    );
  }

  isAuthenticated(): boolean {
    const cookies = document.cookie.split(';');
    for (const cookie of cookies) {
      const [name, value] = cookie.trim().split('=');
      if (name === 'access_token' && value) return true;
    }
    return false;
  }

  getCurrentUser(): Observable<{
    username: string;
    user_db_id?: string;
    department_id?: string;
    department?: string;
  }> {
    return this.http.get<{
      username: string;
      user_db_id?: string;
      department_id?: string;
      department?: string;
    }>(`${this.baseUrl}/api/auth/me`);
  }

  getUsername(): string {
    const cookies = document.cookie.split(';');
    for (const cookie of cookies) {
      const [name, value] = cookie.trim().split('=');
      if (name === 'access_token') {
        try {
          const payload = JSON.parse(atob(value.split('.')[1]));
          return payload.sub || '';
        } catch {
          return '';
        }
      }
    }
    return '';
  }

  getStoredUser(): {
    username: string;
    user_db_id?: string;
    department_id?: string;
    department?: string;
  } | null {
    const stored = localStorage.getItem('user_details');
    if (stored) {
      return JSON.parse(stored);
    }
    return null;
  }

  refreshUserDetails(): void {
    if (!this.isAuthenticated()) return;
    this.getCurrentUser().subscribe({
      next: (user) => {
        localStorage.setItem('user_details', JSON.stringify(user));
      },
      error: (err) => {
        console.error('Failed to refresh user details:', err);
      },
    });
  }

  getUserDetails(): {
    username: string;
    user_db_id?: string;
    department_id?: string;
    department?: string;
  } | null {
    const cookies = document.cookie.split(';');
    for (const cookie of cookies) {
      const [name, value] = cookie.trim().split('=');
      if (name === 'access_token') {
        try {
          const payload = JSON.parse(atob(value.split('.')[1]));
          return {
            username: payload.sub || '',
            user_db_id: payload.user_db_id || '',
            department_id: payload.department_id || '',
            department: payload.department || '',
          };
        } catch {
          return null;
        }
      }
    }
    return null;
  }

  createProperty(
    name: string,
    type: string,
    metadata?: Record<string, unknown>,
  ): Observable<{ id: string; name: string }> {
    return this.http.post<{ id: string; name: string }>(
      `${this.baseUrl}/api/properties`,
      { name, type, metadata },
    );
  }

  getProperties(type: string): Observable<{
    type: string;
    items: { id: string; name: string; archived: boolean }[];
  }> {
    return this.http.get<{
      type: string;
      items: { id: string; name: string; archived: boolean }[];
    }>(`${this.baseUrl}/api/properties/${type}`);
  }
}


/**
 * Login response is one of two shapes (discriminated by `mfa_required`).
 */
export type LoginResponse =
  | {
      mfa_required: true;
      mfa_token: string;
      channels: string[];
      otp_length: number;
      ttl_seconds: number;
    }
  | {
      access_token: string;
      token_type: string;
    };

/**
 * /mfa/request response. `channels` is the list of channels that
 * actually received the code (may be smaller than the request if user
 * doesn't have all requested channels configured).
 */
export interface MfaRequestResponse {
  sent: boolean;
  channels: string[];
  ttl_seconds: number;
}
