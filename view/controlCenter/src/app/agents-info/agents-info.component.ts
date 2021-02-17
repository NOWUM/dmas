import { Component, OnInit } from '@angular/core';
import {HttpClient} from "@angular/common/http";

@Component({
  selector: 'app-agents-info',
  templateUrl: './agents-info.component.html',
  styleUrls: ['./agents-info.component.css']
})
export class AgentsInfoComponent implements OnInit {
  readonly ROOT_URL = 'http://149.201.88.75:5010';
  agentTypes: string[] = ['PWP', 'RES', 'DEM', 'STR', 'MRK', 'NET'];
  agents: Map<string, string>;

  showPwp = true;
  showRes = true;
  DEM = true;
  showStr = true;
  showMrk = true;
  showNet = true;

  constructor(private http: HttpClient) {
    this.agents = new Map<string, string>();
  }

  ngOnInit(): void {

  }

  togglePwp() { this.showPwp = !this.showPwp; }
  toggleRes() { this.showRes = !this.showRes; }
  toggleDem() { this.DEM = !this.DEM; }
  toggleStr() { this.showStr = !this.showStr; }
  toggleMrk() { this.showMrk = !this.showMrk; }
  toggleNet() { this.showNet = !this.showNet; }

  get_agents_test(inType:string = 'NET'){
    let test = 0;
    return test;
  }

  get_agents(inType:string = 'NET'){
    let test = 0;
    this.http.get(this.ROOT_URL + '/get_info/' + inType).subscribe((data: any) => {
      console.log(data);
      for (var value in data) {
        this.agents.set(value, data[value]);
      }
      return test;
    });
  }

}
