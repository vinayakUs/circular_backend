import { TestBed } from '@angular/core/testing';
import { provideHttpClient } from '@angular/common/http';
import { HttpTestingController, provideHttpClientTesting } from '@angular/common/http/testing';

import { AuthService, SessionResponse } from './auth.service';

/**
 * AuthService is the single source of truth for auth state, and the two
 * properties worth protecting are the tri-state status and the
 * single-flight refresh that stops N parallel 401s rotating the refresh
 * token N times.
 */
describe('AuthService', () => {
  let service: AuthService;
  let httpMock: HttpTestingController;

  const SESSION: SessionResponse = {
    authenticated: true,
    refreshed: false,
    access_expires_at: new Date(Date.now() + 900_000).toISOString(),
    csrf_token: 'csrf-1',
    user: { username: 'alice', persona_slug: 'compliance' },
  };

  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [provideHttpClient(), provideHttpClientTesting()],
    });
    service = TestBed.inject(AuthService);
    httpMock = TestBed.inject(HttpTestingController);
  });

  afterEach(() => {
    // Stop the renewal timer, otherwise it fires after the spec ends and
    // issues a request the HttpTestingController no longer controls.
    service.reset();
    httpMock.verify();
  });

  it('starts unknown, not anonymous', () => {
    // Guards must not redirect on 'unknown' — that would bounce a signed-in
    // user to /login during a cold load.
    expect(service.status()).toBe('unknown');
    expect(service.isAuthenticated()).toBeFalse();
  });

  it('becomes authenticated on a 200 session', (done) => {
    service.session().subscribe((s) => {
      expect(s).toEqual(SESSION);
      expect(service.status()).toBe('authenticated');
      expect(service.user()?.username).toBe('alice');
      expect(service.csrf()).toBe('csrf-1');
      done();
    });
    httpMock.expectOne('/api/auth/session').flush(SESSION);
  });

  it('becomes anonymous on 401 and clears the csrf token', (done) => {
    service.csrf();
    service.session().subscribe((s) => {
      expect(s).toBeNull();
      expect(service.status()).toBe('anonymous');
      expect(service.csrf()).toBeNull();
      expect(service.user()).toBeNull();
      done();
    });
    httpMock.expectOne('/api/auth/session')
      .flush(null, { status: 401, statusText: 'Unauthorized' });
  });

  it('leaves status unknown when the network fails', (done) => {
    // A dropped connection is not a logout.
    service.session().subscribe({
      next: () => done.fail('expected an error'),
      error: () => {
        expect(service.status()).toBe('unknown');
        done();
      },
    });
    httpMock.expectOne('/api/auth/session')
      .flush(null, { status: 500, statusText: 'Server Error' });
  });

  it('collapses concurrent refreshes into one /session request', () => {
    let results: (SessionResponse | null)[] = [];
    service.refreshOnce().subscribe((s) => results.push(s));
    service.refreshOnce().subscribe((s) => results.push(s));
    service.refreshOnce().subscribe((s) => results.push(s));

    // One request, not three: six parallel 401s must rotate the refresh
    // token once, not six times.
    httpMock.expectOne('/api/auth/session').flush(SESSION);
    expect(results).toEqual([SESSION, SESSION, SESSION]);
  });

  it('allows a fresh refresh after the previous one settles', () => {
    service.refreshOnce().subscribe();
    httpMock.expectOne('/api/auth/session').flush(SESSION);

    service.refreshOnce().subscribe();
    httpMock.expectOne('/api/auth/session').flush(SESSION);
  });

  it('reset clears local state without touching the server', () => {
    service.session().subscribe();
    httpMock.expectOne('/api/auth/session').flush(SESSION);

    service.reset();
    expect(service.status()).toBe('anonymous');
    expect(service.user()).toBeNull();
    expect(service.csrf()).toBeNull();
    httpMock.expectNone('/api/auth/session');
  });

  it('adopt applies a session returned by /mfa/verify', () => {
    service.adopt(SESSION);
    expect(service.status()).toBe('authenticated');
    expect(service.csrf()).toBe('csrf-1');
    httpMock.expectNone('/api/auth/session');
  });

  describe('expiry awareness', () => {
    const inMs = (ms: number) => new Date(Date.now() + ms).toISOString();

    it('reports not authenticated once the access token has expired', (done) => {
      // The "both tokens died while I sat on a persona page" case: nothing
      // has 401'd, but the UI must stop claiming a live session.
      const stale = { ...SESSION, access_expires_at: inMs(-1_000) };
      service.adopt(stale);

      service.session().subscribe(() => {
        expect(service.status()).toBe('authenticated');
        expect(service.expired()).toBeTrue();
        expect(service.isAuthenticated()).toBeFalse();
        done();
      });
      httpMock.expectOne('/api/auth/session').flush(stale);
    });

    it('still reports authenticated while the token is live', () => {
      service.adopt({ ...SESSION, access_expires_at: inMs(900_000) });
      expect(service.expired()).toBeFalse();
      expect(service.isAuthenticated()).toBeTrue();
    });

    it('parses an expiry carrying sub-millisecond precision', () => {
      // Python isoformat() emits six fractional digits; a NaN parse would
      // silently disable the whole expiry check.
      const micro = new Date(Date.now() + 900_000).toISOString().replace('Z', '.582498+00:00');
      service.adopt({ ...SESSION, access_expires_at: micro });
      expect(service.expired()).toBeFalse();
      expect(service.isAuthenticated()).toBeTrue();
    });

    it('treats an unparseable expiry as no expiry rather than expired', () => {
      service.adopt({ ...SESSION, access_expires_at: 'not-a-date' });
      expect(service.expired()).toBeFalse();
    });
  });
});
