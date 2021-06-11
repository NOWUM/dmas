/* info.component.ts
|   Info: Running Agent Info Komponente
|   Typ: TS Logic
|   Inhalt: Logic to receive a list of running Agents and terminate them for each type
*/

import { Component, OnInit, EventEmitter, Input, Output } from '@angular/core';
import {ConfigService} from "../config.service";

@Component({
  selector: 'app-info',
  templateUrl: './info.component.html',
  styleUrls: ['./info.component.css']
})
export class InfoComponent implements OnInit {

  @Input() type: string;
  @Output() home: EventEmitter<string>;

  agents: Map<string, string>;

  constructor(public service: ConfigService) {
    this.type = 'PWP';
    this.agents = new Map<string, string>();
    this.home = new EventEmitter<string>();
  }

  ngOnInit(): void {
    // Get Agent Info on loading
    this.get_info();
  }

  // Get a list of running Agents
  get_info(){
    this.service.get_info(this.type).subscribe((data: any) => {
      console.log(data);
      for (var value in data) {
        this.agents.set(value, data[value]);
      }
    });
  }

  // Terminate Agent specified by its key (e.g. "DEM_40)
  terminate_agent(key: string): void {
    console.log(key);
    this.service.terminate_agent(key).subscribe((data: any) => {
      console.log(data);
      this.agents.delete(key);
    });
  }

}
