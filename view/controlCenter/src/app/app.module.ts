import { BrowserModule } from '@angular/platform-browser';
import { NgModule } from '@angular/core';
import { HttpClientModule } from '@angular/common/http';
import { FormsModule } from '@angular/forms';

import { AppComponent } from './app.component';
import { ConfigComponent } from './config/config.component';
import { InfoComponent } from './info/info.component';
import { GridComponent } from './grid/grid.component';

@NgModule({
  declarations: [
    AppComponent,
    ConfigComponent,
    InfoComponent,
    GridComponent
  ],
  imports: [
    BrowserModule,
    HttpClientModule,
    FormsModule
  ],
  providers: [],
  bootstrap: [AppComponent]
})
export class AppModule { }
