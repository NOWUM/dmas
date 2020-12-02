import {Component, OnInit, EventEmitter, Input, Output} from '@angular/core';
import {HttpClient} from '@angular/common/http';

@Component({
  selector: 'app-config',
  templateUrl: './config.component.html',
  styleUrls: ['./config.component.css']
})
export class ConfigComponent implements OnInit {

  @Input() type: string;
  @Output() info: EventEmitter<string>;

  readonly ROOT_URL = 'http://149.201.88.75:5010';
  config: Map<string, string>;
  number: number;

  constructor(private http: HttpClient) {
    this.type = 'services';
    this.config = new Map<string, string>();
    this.info = new EventEmitter<string>();
    this.number = 0
  }

  // get data for the first time
  ngOnInit(): void {
    // get config values from config file on Server
    this.http.get(this.ROOT_URL + '/get_config/' + this.type).subscribe((data: any) => {
      // console.log(data);
      for (var value in data) {
        this.config.set(value, data[value]);
      }
    });
    // get running agents from server (mongodb)
    this.http.get(this.ROOT_URL + '/get_running_agents/' + this.type).subscribe((data: any) => {
      //console.log(data);
      this.number = data;
    });
  }
  // send config data to server
  set_config(): void {
    // create json object from map
    let jsonObject: any = {};
    this.config.forEach((value, key) => {
      jsonObject[key] = value;
    });
    // build body and header
    const body = JSON.stringify(jsonObject);
    const headers = {'content-type': 'application/json'};
    // console.log(body);
    // post data to server
    this.http.post(this.ROOT_URL + '/set_config/' + this.type, body, {'headers': headers}).subscribe(data => {
      console.log(JSON.stringify(data));
    })

  }
  // update internal config map
  update_config(key: string, value: string): void {
    this.config.set(key, value);
  }
  // get number of running agents per typ
  get_running_agents(): void {
    // get running agents from server (mongodb)
    this.http.get(this.ROOT_URL + '/get_running_agents/' + this.type).subscribe((data: any) => {
      console.log(data);
      this.number = data;
    });
  }
  // terminate agents per typ
  terminate_agents(): void {
    this.http.get(this.ROOT_URL + '/terminate_agents/' + this.type).subscribe((data: any) => {
      console.log(data);
    });
  }
  // start agents per typ
  start_agents(): void {
    this.http.get(this.ROOT_URL + '/start_agents/' + this.type).subscribe((data: any) => {
      console.log(data);
    });
  }
  // emit event to switch to info page
  get_info(): void {
    this.info.emit(this.type);
  }



}
