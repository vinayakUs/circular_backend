import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Observable } from 'rxjs';
import { environment } from 'src/environments/environment';

export interface UserRecord {
  id: string;
  user_id: string;
  department_id: string;
  created_at: string;
  created_by: string;
  updated_at: string | null;
  updated_by: string | null;
}

export interface Department {
  id: string;
  name: string;
  archived: boolean;
}

@Injectable({
  providedIn: 'root'
})
export class UserService {
  private baseUrl = environment.apiUrl;

  constructor(private http: HttpClient) { }

  getDepartments(): Observable<{ type: string, items: Department[] }> {
    return this.http.get<{ type: string, items: Department[] }>(
      `${this.baseUrl}/api/properties/department`
    );
  }

  getDepartmentUsers(deptId: string): Observable<{ users: UserRecord[] }> {
    return this.http.get<{ users: UserRecord[] }>(
      `${this.baseUrl}/api/admin/departments/${deptId}/users`
    );
  }

  addUserToDepartment(deptId: string, userId: string): Observable<UserRecord> {
    return this.http.post<UserRecord>(
      `${this.baseUrl}/api/admin/departments/${deptId}/users`,
      { user_id: userId }
    );
  }

  removeUserFromDepartment(deptId: string, userId: string): Observable<{ success: boolean }> {
    return this.http.delete<{ success: boolean }>(
      `${this.baseUrl}/api/admin/departments/${deptId}/users/${userId}`
    );
  }
}
