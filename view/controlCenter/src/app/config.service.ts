/*
/ config.service.ts:
/ contains all methods used for http connections
/ used in several components
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
  //number: number;
  start: string = '2018-01-01';
  end: string = '2018-12-31';

  constructor(private http: HttpClient) {
    //this.type = 'services';
    this.config = new Map<string, string>();
    this.info = new EventEmitter<string>();
    //this.number = 0
  }

  // get data for the first time
  ngOnInit(): void {
    //this.config = new Map<string, string>();
    // // get config values from config file on Server
    // this.http.get(this.ROOT_URL + '/get_config/' + this.type).subscribe((data: any) => {
    //   console.log(data);
    //   for (var value in data) {
    //     this.config.set(value, data[value]);
    //   }
    // });
    // // get running agents from server (mongodb)
    // this.http.get(this.ROOT_URL + '/get_running_agents/' + this.type).subscribe((data: any) => {
    //   console.log(data);
    //   this.number = data;
    // });
  }

  // //// Seems to not work like this due to singelton -> changes apply to all returned objects
  // get_config(type:string = 'test'): void {
  //   // get config values from config file on Server
  //   this.http.get(this.ROOT_URL + '/get_config/' + type).subscribe((data: any) => {
  //     console.log(data);
  //     for (var value in data) {
  //       this.config.set(value, data[value]);
  //     }
  //   });
  // }

  //get_config(type:string = 'test'): void {
  //get_config(retConfig:Map<string, string> = new Map<string, string>(), type:string = 'test'):  Map<string, string> {//klappt
  get_config_v1(inType:string = 'test'):  Map<string, string> {//klappt
  // get config values from config file on Server
    let retConfig = new Map<string, string>();
    console.log('inType:'+ inType);
    this.http.get(this.ROOT_URL + '/get_config/' + inType).subscribe((data: any) => {
      console.log('get_config:');
      console.log(data);
      for (var value in data) {
        //this.config.set(value, data[value]); //seems to not work like this due to singelton -> changes apply to all returned objects
        retConfig.set(value, data[value]);
      }
    });
    //return this.config; //seems to not work like this due to singelton -> changes apply to all returned objects
    return retConfig;
  }

    get_config(inType:string = 'test') {//klappt
  // get config values from config file on Server
    let retConfig = new Map<string, string>();
    console.log('get_config: '+ inType);
    return this.http.get<Map<string, string>>(this.ROOT_URL + '/get_config/' + inType);
  }

        // get number of running agents per typ
  get_running_agents(inType:string = 'test') {
    console.log('get_running_agents: ' + inType);
    // get running agents from server (mongodb)
    return this.http.get<number>(this.ROOT_URL + '/get_running_agents/' + inType);
  }



  // // (orig) send config data to server
  // set_config_orig(): void {
  //   // create json object from map
  //   let jsonObject: any = {};
  //   this.config.forEach((value, key) => {
  //     jsonObject[key] = value;
  //   });
  //   // build body and header
  //   const body = JSON.stringify(jsonObject);
  //   const headers = {'content-type': 'application/json'};
  //   console.log(body);
  //   // post data to server
  //   this.http.post(this.ROOT_URL + '/set_config/' + this.type, body, {'headers': headers}).subscribe(data => {
  //     console.log(JSON.stringify(data));
  //   })
  // }

   // send config data to server
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

  //TODO: evtl. Methode für get_grid hier implementieren, um das Grid Json Object vom server abzurufen? -> danach in grid.component zeichnen
    get_grid(inType:string = 'test') {
    console.log('get_grid');
    // get running agents from server (mongodb)
    return this.http.get<number>('http://149.201.88.70:7777/Grid');
  }

  // // update internal config map
  // update_config(key: string, value: string): void {
  //   this.config.set(key, value);
  //   console.log(this.config);
  // }

  // // get number of running agents per typ
  // get_running_agents_orig(inType:string = 'test'): void {
  //   console.log('get_running_agents:' + inType);
  //   // get running agents from server (mongodb)
  //   this.http.get(this.ROOT_URL + '/get_running_agents/' + inType).subscribe((data: any) => {
  //     console.log(data);
  //     this.number = data;
  //   });
  // }

  // //Test
  //   // get number of running agents per typ
  // get_running_agents(inType:string = 'test', outNumber:number = 0): void {
  //   console.log('get_running_agents:' + inType);
  //   // get running agents from server (mongodb)
  //   this.http.get(this.ROOT_URL + '/get_running_agents/' + inType).subscribe((data: any) => {
  //     console.log(data);
  //     outNumber = data;
  //   });
  // }

  // //test return with async function
  //   // get number of running agents per typ
  // async get_running_agents(inType:string = 'test'): Promise<number> {
  //   console.log('get_running_agents:' + inType);
  //   let retNumber = -1;
  //   // get running agents from server (mongodb)
  //   await this.http.get(this.ROOT_URL + '/get_running_agents/' + inType).subscribe((data: any) => {
  //     console.log(data);
  //     retNumber = data;
  //   });
  //
  //   let result = await Promise.resolve(this.http.get(this.ROOT_URL + '/get_running_agents/' + inType).subscribe((data: any)=> {
  //     console.log(data);
  //     retNumber = data;
  //     }));
  //
  //   //async -> TODO: auf http get warten bevor retNumber zurückgegeben wird!
  //   console.log('retNumber: ' + retNumber);
  //   console.log('result: ' + result);
  //   return retNumber;
  //   //return 1;
  // }



  // public fetchData(inType:string = 'test'):number{
  //   const promise = this.http.get(this.ROOT_URL + '/get_running_agents/' + inType).toPromise();
  //   console.log(promise);
  //   promise.then((data)=>{
  //     console.log("Promise resolved with: " + JSON.stringify(data));
  //     return 1337;
  //   }).catch((error)=>{
  //     console.log("Promise rejected with " + JSON.stringify(error));
  //     return 1337;
  //   });
  //   return -2;
  // }

  // terminate agents per typ
  terminate_agents(inType:string = 'test'): void {
    console.log('terminate_agents:' + inType);
    this.http.get(this.ROOT_URL + '/terminate_agents/' + inType).subscribe((data: any) => {
      console.log(data);
    });
  }
  // start agents per typ
  start_agents(inType:string = 'test'): void {
    console.log('start_agents:' + inType);
    this.http.get(this.ROOT_URL + '/start_agents/' + inType).subscribe((data: any) => {
      console.log(data);
    });
  }
  // emit event to switch to info page
  get_info(inType:string = 'test'): void {
    this.info.emit(inType);
    //TODO: Implement get_info_page, maybe inside ConfigAgentsComponent? -> Return new page or div with info about running agents
  }

  start_simulation(){
    let jsonObject: any = {};
    jsonObject['start'] = this.start;
    jsonObject['end'] = this.end;

    console.log(jsonObject);
    const body = JSON.parse(JSON.stringify(jsonObject));
    const headers = {'content-type': 'application/json'};

    this.http.post(this.ROOT_URL + '/start_simulation', body, {'headers': headers}).subscribe(data => {
      console.log(JSON.stringify(data));
    })
  }

}
