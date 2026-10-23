import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Observable } from 'rxjs';
import { environment } from 'src/environments/environment';

import { SessionResponse } from './auth.service';

/**
 * Drives the login/MFA handshake. Owns no auth state — the access token is
 * HttpOnly, so AuthService holds the state and this only establishes it.
 */
@Injectable({ providedIn: 'root' })
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
   * Login. The server always answers with the MFA challenge shape; there
   * is no direct-JWT branch.
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
   * Verify the OTP code. On success the server sets all three auth cookies
   * and returns the session envelope; the token itself is never in the body.
   */
  mfaVerify(mfaToken: string, otp: string): Observable<SessionResponse> {
    return this.http.post<SessionResponse>(
      `${this.baseUrl}/api/auth/mfa/verify`,
      { mfa_token: mfaToken, otp },
    );
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

/** /login response — always the MFA challenge shape. */
export interface LoginResponse {
  mfa_required: true;
  mfa_token: string;
  channels: string[];
  otp_length: number;
  ttl_seconds: number;
}

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
