const fs=require('fs'); const extracted=fs.readFileSync('extracted.js','utf8');
let rows=[];
function makeEl(){return{style:{},_in:"",set innerHTML(v){this._in=v},get innerHTML(){return this._in},appendChild(c){rows.push(c)},querySelector(sel){if(sel.indexOf("theme-select")>=0)return this._in.indexOf("customer-theme-select")>=0?{addEventListener(){}}:null;return{addEventListener(){}};}};}
const listContainer=makeEl();
const escHtml=s=>String(s).replace(/[&<>"]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));
let activeCustomer="",customerThemes={ACME:"wipro"},CURTHEME="wipro";
function curTheme(){return CURTHEME;}
const selectCustomer=()=>{},deleteCustomer=()=>{},reassignCustomerTheme=()=>{};
const document={createElement(){return makeEl();}};
eval(extracted);
function run(t){CURTHEME=t;rows=[];renderCustomerList(["ACME"]);return rows[0].innerHTML;}
const wip=run("wipro"),ever=run("everpure");
console.log("WIPRO: badge+label:", /class="badge"/.test(wip)&&/Wipro/.test(wip), "| dropdown hidden:", !/customer-theme-select/.test(wip));
console.log("EVERPURE: badge+label:", /class="badge"/.test(ever)&&/Wipro/.test(ever), "| dropdown present:", /customer-theme-select/.test(ever));
console.log("\n--- EVERPURE row ---\n"+ever.split("\n").filter(l=>l.includes("badge")||l.includes("theme-select")).join("\n"));
