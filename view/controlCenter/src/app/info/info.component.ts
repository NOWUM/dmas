import { Component, OnInit, EventEmitter, Input, Output } from '@angular/core';
import { HttpClient } from '@angular/common/http';

@Component({
  selector: 'app-info',
  templateUrl: './info.component.html',
  styleUrls: ['./info.component.css']
})
export class InfoComponent implements OnInit {

  @Input() type: string;
  @Output() home: EventEmitter<string>;

  readonly ROOT_URL = 'http://127.0.0.1:6888';
  agents: Map<string, string>;

  constructor(private http: HttpClient) {
    this.type = 'PWP';
    this.agents = new Map<string, string>();
    this.home = new EventEmitter<string>();
  }

  ngOnInit(): void {
    this.http.get(this.ROOT_URL + '/get_info/' + this.type).subscribe((data: any) => {
      console.log(data);
      for (var value in data) {
        this.agents.set(value, data[value]);
      }
    });
  }

  terminate_agent(key: string): void {
    console.log(key);
    this.http.get(this.ROOT_URL + '/terminate_agent/' + key).subscribe((data: any) => {
      console.log(data);
      this.agents.delete(key);
    });
  }

  get_home(): void {
    this.home.emit('config')
  }

}
