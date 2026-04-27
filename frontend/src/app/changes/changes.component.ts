import { Component, OnInit } from '@angular/core';
import { CommonModule, DatePipe } from '@angular/common';
import { NavbarComponent } from '../navbar/navbar.component';

interface ActionItem {
  date: string;
  title: string;
  type: 'deadline' | 'implementation' | 'compliance';
  source: string;
}

@Component({
  selector: 'app-changes',
  standalone: true,
  imports: [CommonModule, NavbarComponent, DatePipe],
  templateUrl: './changes.component.html',
  styleUrl: './changes.component.css'
})
export class ChangesComponent implements OnInit {
  currentDate = new Date();
  currentMonth = this.currentDate.getMonth();
  currentYear = this.currentDate.getFullYear();

  selectedDate: Date | null = null;
  selectedItems: ActionItem[] = [];

  calendarDays: (number | { day: number; items: ActionItem[] })[] = [];

  actionItems: ActionItem[] = [
    { date: '2026-04-25', title: 'AIF Phase 2 Guidelines Implementation', type: 'implementation', source: 'SEBI' },
    { date: '2026-04-28', title: 'Margin Collection for Currency Derivatives', type: 'deadline', source: 'NSE' },
    { date: '2026-04-30', title: 'Gold ETF Disclosure Requirements', type: 'compliance', source: 'SEBI' },
    { date: '2026-05-05', title: 'KYC Updates for Mutual Fund Investors', type: 'compliance', source: 'NSE' },
    { date: '2026-05-10', title: 'Options Trading Risk Parameters', type: 'implementation', source: 'NSE' },
    { date: '2026-05-15', title: 'AIF Investment Guidelines Final Phase', type: 'deadline', source: 'SEBI' },
    { date: '2026-05-20', title: 'Commodity Derivatives Margining Norms', type: 'implementation', source: 'MCX' },
    { date: '2026-06-01', title: 'Index Derivatives Contract Rollout', type: 'implementation', source: 'NSE' },
    { date: '2026-06-15', title: 'Mutual Fund Redemption Timings Update', type: 'compliance', source: 'SEBI' },
  ];

  ngOnInit(): void {
    this.generateCalendar();
  }

  generateCalendar(): void {
    const firstDay = new Date(this.currentYear, this.currentMonth, 1);
    const lastDay = new Date(this.currentYear, this.currentMonth + 1, 0);
    const startDay = firstDay.getDay();
    const daysInMonth = lastDay.getDate();

    this.calendarDays = [];

    for (let i = 0; i < startDay; i++) {
      this.calendarDays.push(-1);
    }

    for (let day = 1; day <= daysInMonth; day++) {
      const dateStr = this.formatDateString(this.currentYear, this.currentMonth, day);
      const items = this.actionItems.filter(item => item.date === dateStr);
      this.calendarDays.push({ day, items });
    }
  }

  formatDateString(year: number, month: number, day: number): string {
    return `${year}-${String(month + 1).padStart(2, '0')}-${String(day).padStart(2, '0')}`;
  }

  isToday(day: number | { day: number; items: ActionItem[] }): boolean {
    if (typeof day === 'number') return false;
    const today = new Date();
    return day.day === today.getDate() &&
           this.currentMonth === today.getMonth() &&
           this.currentYear === today.getFullYear();
  }

  isSelected(day: number | { day: number; items: ActionItem[] }): boolean {
    if (typeof day === 'number' || !this.selectedDate) return false;
    return day.day === this.selectedDate.getDate() &&
           this.currentMonth === this.selectedDate.getMonth() &&
           this.currentYear === this.selectedDate.getFullYear();
  }

  onDayClick(day: number | { day: number; items: ActionItem[] }): void {
    if (typeof day === 'number') return;
    this.selectedDate = new Date(this.currentYear, this.currentMonth, day.day);
    this.selectedItems = day.items;
  }

  previousMonth(): void {
    if (this.currentMonth === 0) {
      this.currentMonth = 11;
      this.currentYear--;
    } else {
      this.currentMonth--;
    }
    this.generateCalendar();
    this.selectedItems = [];
    this.selectedDate = null;
  }

  nextMonth(): void {
    if (this.currentMonth === 11) {
      this.currentMonth = 0;
      this.currentYear++;
    } else {
      this.currentMonth++;
    }
    this.generateCalendar();
    this.selectedItems = [];
    this.selectedDate = null;
  }

  goToToday(): void {
    this.currentMonth = new Date().getMonth();
    this.currentYear = new Date().getFullYear();
    this.generateCalendar();
    this.selectedItems = [];
    this.selectedDate = null;
  }

  get monthName(): string {
    const months = [
      'January', 'February', 'March', 'April', 'May', 'June',
      'July', 'August', 'September', 'October', 'November', 'December'
    ];
    return months[this.currentMonth];
  }

  getDayNumber(day: number | { day: number; items: ActionItem[] }): number {
    return typeof day === 'number' ? day : day.day;
  }

  isEmptyDay(day: number | { day: number; items: ActionItem[] }): boolean {
    return typeof day === 'number' || day.items.length === 0;
  }

  getDayItems(day: number | { day: number; items: ActionItem[] }): ActionItem[] {
    if (typeof day === 'number') return [];
    return day.items.slice(0, 3);
  }

  getTypeColor(type: string): string {
    switch (type) {
      case 'deadline': return '#ef4444';
      case 'implementation': return '#3b82f6';
      case 'compliance': return '#22c55e';
      default: return '#9e9e9e';
    }
  }

  getTypeLabel(type: string): string {
    switch (type) {
      case 'deadline': return 'Deadline';
      case 'implementation': return 'Implementation';
      case 'compliance': return 'Compliance';
      default: return type;
    }
  }
}