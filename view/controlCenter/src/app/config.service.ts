/*
/ config.service.ts:
/ Communication with the Server Backend
/ contains all methods used for http connections (sending to and receiving from server backend)
/ used in several components
/ Singleton: modifications apply to all instances
*/


import {EventEmitter, Injectable, Input, OnInit, Output} from '@angular/core';
import {HttpClient} from "@angular/common/http";

@Injectable({
  providedIn: 'root'
})
export class ConfigService implements OnInit{

  //@Input() type: string;
  @Output() info: EventEmitter<string>;

  readonly ROOT_URL = 'http://149.201.88.75:5010';
  config: Map<string, string>;
  start: string = '2018-01-01';
  end: string = '2018-12-31';

  constructor(private http: HttpClient) {
    this.config = new Map<string, string>();
    this.info = new EventEmitter<string>();
  }

  // get data for the first time
  ngOnInit(): void {
  }

  // Get config values from config file on Server
  get_config(inType:string = 'test') {
    console.log('service.get_config(): '+ inType);
    return this.http.get<Map<string, string>>(this.ROOT_URL + '/get_config/' + inType);
  }

  get_info(inType:string = 'test'){
    console.log('service.get_info(): '+ inType);
    return this.http.get(this.ROOT_URL + '/get_info/' + inType);
  }

  // Get number of running agents per typ from server backend
  get_running_agents(inType:string = 'test') {
    console.log('service.get_running_agents(): ' + inType);
    return this.http.get<number>(this.ROOT_URL + '/get_running_agents/' + inType);
  }

  // Send config data to server backend
  set_config(inConfig:Map<string, string> = new Map<string, string>(), inType:string = 'test'): void {
    // create json object from map
    let jsonObject: any = {};
    inConfig.forEach((value, key) => {
      jsonObject[key] = value;
    });
    // build body and header
    const body = JSON.stringify(jsonObject);
    const headers = {'content-type': 'application/json'};
    console.log(body);
    // post data to server
    this.http.post(this.ROOT_URL + '/set_config/' + inType, body, {'headers': headers}).subscribe(data => {
      console.log(JSON.stringify(data));
    })
  }



  // terminate agents per typ
  terminate_agents(inType:string = 'test'): void {
    console.log('service.terminate_agents():' + inType);
    this.http.get(this.ROOT_URL + '/terminate_agents/' + inType).subscribe((data: any) => {
      console.log(data);
    });
  }

  // terminate agents per typ
  terminate_agent(inKey:string = 'test') {
    console.log('service.terminate_agent():' + inKey);
    return this.http.get(this.ROOT_URL + '/terminate_agent/' + inKey);
  }



  // start agents per typ
  start_agents(inType:string = 'test'): void {
    console.log('service.start_agents():' + inType);
    this.http.get(this.ROOT_URL + '/start_agents/' + inType).subscribe((data: any) => {
      console.log(data);
    });
  }

  // Übergabe der Startparameter (start/end) und des Startbefehls für die Simulation an das Server Backend
  start_simulation(){
    // create json object
    let jsonObject: any = {};
    jsonObject['start'] = this.start;
    jsonObject['end'] = this.end;
    console.log(jsonObject);
    // build body and header
    const body = JSON.parse(JSON.stringify(jsonObject));
    const headers = {'content-type': 'application/json'};
    // post data to server
    this.http.post(this.ROOT_URL + '/start_simulation', body, {'headers': headers}).subscribe(data => {
      console.log(JSON.stringify(data));
    })
  }

  // // emit event to switch to info page
  // get_info(inType:string = 'test'): void {
  //   this.info.emit(inType);
  //   //TODO: Implement get_info_page, maybe inside ConfigAgentsComponent? -> Return new page or div with info about running agents
  // }

  // //TODO: evtl. Methode für get_grid hier implementieren, um das Grid Json Object vom server abzurufen? -> danach in grid.component zeichnen
  // get_grid(inType:string = 'test') {
  //   console.log('service.get_grid()');
  // }

}
