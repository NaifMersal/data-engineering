/* Original Qafila story. Recurring illustrations preserve continuity across the evolving architecture. */
(() => {
'use strict';
const root=document.getElementById('stage');
if(location.search.includes('check'))document.body.classList.add('checking');
const sources={reference:'https://www.youtube.com/watch?v=Grx95nfsz08',aws:'https://aws.amazon.com/compare/the-difference-between-a-data-warehouse-data-lake-and-data-mart/',lakehouse:'https://docs.databricks.com/aws/en/lakehouse/',iceberg:'https://iceberg.apache.org/docs/latest/reliability/'};
const art=(kind,x,y,w,title='',detail='',id=kind)=>`<figure class="figure" data-id="${id}" style="left:${x}px;top:${y}px;width:${w}px;margin:0"><div class="art ${kind}" aria-hidden="true"></div>${title?`<figcaption>${title}</figcaption>`:''}${detail?`<p class="detail">${detail}</p>`:''}</figure>`;
const label=(text,x,y,cls='',w=300,id='')=>`<div class="label ${cls}" ${id?`data-id="${id}"`:''} style="left:${x}px;top:${y}px;width:${w}px">${text}</div>`;
const note=(text,x,y,cls='',w=320)=>`<p class="annotation ${cls}" style="left:${x}px;top:${y}px;width:${w}px">${text}</p>`;
const path=(d,cls='')=>`<path class="wire ${cls}" d="${d}"/>`;
const outline=(x,y,w,h,cls='')=>`<path class="enclosure ${cls}" d="M${x+11} ${y} L${x+w-6} ${y+2} Q${x+w+2} ${y+3} ${x+w} ${y+13} L${x+w-2} ${y+h-9} Q${x+w-1} ${y+h+2} ${x+w-12} ${y+h} L${x+5} ${y+h-1} Q${x-2} ${y+h} ${x} ${y+h-13} L${x+1} ${y+9} Q${x} ${y-1} ${x+11} ${y}Z"/>`;
let svgCount=0;
const svg=content=>{const id=svgCount++;return `<svg class="wires" viewBox="0 0 1440 510" aria-hidden="true"><defs>${[['normal','#61717c'],['em','#348f8d'],['danger','#c46558']].map(([k,c])=>`<marker id="arr-${id}-${k}" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="7" markerHeight="7" orient="auto"><path d="M1 1L9 5L1 9" fill="none" stroke="${c}" stroke-width="1.3"/></marker>`).join('')}</defs>${content.replace(/class="wire([^"]*)"/g,(_,cls)=>`class="wire${cls}" marker-end="url(#arr-${id}-${cls.includes('em')?'em':cls.includes('danger')?'danger':'normal'})"`)}</svg>`;};
const box=(title,body,x,y,w,cls='',id=title,tag='')=>`<div class="stage-box ${cls}" data-id="${id}" style="left:${x}px;top:${y}px;width:${w}px"><h3>${title}</h3>${body}${tag?`<span class="tag">${tag}</span>`:''}</div>`;
const src=(files=false)=>label('SOURCE SYSTEMS',22,0,'hand muted',300,'sources-title')+art('app',10,60,125,'Qafila app','','app')+art('db',160,73,155,'Orders + payments','','source-db')+(files?art('files',40,270,170,'Events + reviews','','source-files'):'');
const users=(both=false)=>label('CONSUMERS',1180,0,'hand muted',250,'users-title')+art('analyst',1200,55,165,'Analysts','Reports + BI','analyst')+(both?art('engineer',1200,305,150,'Data scientists','Explore + ML','scientist'):'');
const warehouse=(layers=false)=>label('DATA WAREHOUSE',504,30,'hand blue',480,'warehouse-title')+(layers?box('Staging','<p>Source tables</p>',420,140,175,'','staging')+box('Core','<p>Shared entities</p>',640,140,175,'','core')+box('Marts','<p>Business models</p>',860,140,175,'','marts'):art('warehouse',610,113,220,'Shared analytical tables','','warehouse'));
const scenes=[];
const add=(chapter,title,subtitle,body,takeaway,notes,refs=[],extra={})=>scenes.push({chapter,title,subtitle,body,takeaway,notes,refs,...extra});

add('The question','Warehouse, lake, lakehouse','',svg(path('M260 130 C350 130 350 190 420 210')+path('M500 380 C500 450 420 460 330 450'))+art('warehouse',45,5,215,'Warehouse','','cover-warehouse')+art('lake',420,135,240,'Lake','','cover-lake')+art('house',90,290,230,'Lakehouse','','cover-house'),
'A visual story about what changes as Qafila’s data needs grow.',
'About 15 minutes. Qafila is a fictional grocery delivery company. Follow changing requirements, not a compulsory migration ladder. Ask learners which term they have heard. Arrow keys navigate, N opens notes, O opens overview, F enters fullscreen. The original illustrations intentionally recur to preserve visual continuity.',['reference'],{cover:true});

add('01 / Scattered answers','Everyone builds a different report','Qafila’s teams query the same sources and apply their own rules.',
svg(path('M310 160 C530 20 800 30 1160 120')+path('M310 185 C620 150 800 220 1165 300')+path('M300 200 C650 375 840 390 990 410'))+src()+art('analyst',1190,30,150,'Finance','','finance')+art('analyst',1190,235,150,'Operations','','ops')+art('engineer',1000,320,140,'Another extract','','extra')+note('Refunds included?',520,20,'red')+note('Cancelled orders?',600,160,'red')+note('Whose revenue is right?',450,400,'red',390),
'<strong>The problem:</strong> each report repeats integration work and chooses its own definition.',
'Two correctly calculated numbers can disagree because they answer different questions. In this hypothetical example finance subtracts refunds while operations counts created orders. Central storage alone cannot settle the definition.');

add('02 / The warehouse','A shared place for analytical tables','A data engineer prepares reusable models. Analysts query the common result.',
svg(outline(395,58,660,355,'blue')+path('M315 176 C350 176 355 176 393 176','em')+path('M1060 175 L1180 175','em'))+src()+warehouse()+users()+art('engineer',870,200,125,'Data engineer','','engineer')+note('Agree the meaning once',475,420,'teal',450)+label('load + model',332,102,'hand muted',240),
'<strong>Warehouse:</strong> an analytical database with shared tables for reporting.',
'The sources stay in place. Introduce the warehouse in the middle. In ETL data transforms before the target load. In ELT raw data can land first and transform inside the warehouse. The team still owns definitions and quality.',['aws']);

add('02 / Inside the warehouse','Staging, core, marts','The layers separate source cleanup from shared entities and business use.',
svg(outline(395,58,680,355,'blue')+path('M315 185 L412 185')+path('M598 225 L631 225','em')+path('M818 225 L852 225','em')+path('M1078 180 L1180 175'))+src()+warehouse(true)+users()+note('Land the<br>source',425,348,'center',175)+note('Join the<br>business',640,348,'center',175)+note('Serve a<br>question',866,348,'center',175),
'<strong>One platform, several layers.</strong> Each layer makes a different promise.',
'These are example warehouse layer names, not universal requirements. Staging retains source structure and normalizes types. Core integrates business entities. Marts serve particular analytical needs. The reference video uses Stage, Core, Mart.',['reference']);

add('03 / A new requirement','The next question needs new evidence','“Why are some deliveries late?” Revenue tables alone cannot answer it.',
svg(outline(395,58,660,355,'blue')+path('M315 175 L393 175')+path('M260 360 C350 360 360 280 393 280','danger')+path('M1060 175 L1180 175'))+src(true)+warehouse()+users()+note('Courier event history',335,395,'red',370)+note('Support text + photos',335,440,'red',370)+note('We did not retain<br>this detail',900,410,'red',350),
'<strong>The pressure:</strong> retain diverse source detail before every future question is known.',
'This is a limitation of Qafila’s current model and retention choices. Modern warehouses can process JSON, text, large histories and ML workloads. A lake is an option for retaining broad source detail. Ask what is lost when only daily aggregates remain.');

add('04 / The data lake','A home for the original evidence','Land files in object storage, then prepare them for each use case.',
svg(outline(395,58,660,385,'teal')+path('M315 175 C420 175 450 190 542 210','em')+path('M240 350 C370 350 435 320 542 285','em')+path('M1060 175 L1180 175')+path('M1060 310 C1110 310 1140 360 1180 360'))+src(true)+users(true)+label('DATA LAKE',530,25,'hand teal',450,'lake-title')+art('lake',535,115,255,'Object storage','','lake')+label('JSON    text    Parquet',560,415,'muted',410,'formats')+note('Keep it now.<br>Shape it when needed.',830,195,'teal',210),
'<strong>Lake:</strong> a repository of raw and curated files that processing engines can read.',
'SQL engines and Python process the files. A lake may have well-governed curated datasets. The warehouse can remain if it serves reporting well. Retaining evidence preserves options, subject to access and retention policies.',['aws']);

add('04 / A folder is not a transaction','Which version did the dashboard read?','A job replaces several Parquet files while a query reads the folder.',
svg(path('M330 205 L495 205')+path('M950 205 L1150 205','danger'))+art('engineer',70,105,210,'Writer','','writer')+box('orders/','<div class="data-title"><span>File</span><span>Version</span></div><div class="data-row"><span>part-1</span><span style="color:#268b7e">new</span></div><div class="data-row"><span>part-2</span><span>old</span></div>',510,100,390,'','folder')+art('analyst',1170,105,210,'Reader','','reader')+note('The first file has changed…',415,15,'',480)+note('…the second has not.',680,380,'red',400)+note('A mixed result',1100,390,'red',300),
'<strong>The gap:</strong> plain files alone do not publish a whole table update atomically.',
'This is a deliberately unsafe multi-file update. Atomic individual writes and strongly consistent object storage do not themselves define a multi-file table transaction. Ask whether a bigger server changes that guarantee.',['iceberg']);

add('05 / Two systems to operate','A lake beside a warehouse','One team explores source files. Another serves modeled reports.',
svg(outline(400,35,650,200,'blue')+outline(400,275,650,200,'teal')+path('M315 170 C350 170 355 145 395 145')+path('M315 175 C350 175 355 365 395 365')+path('M1055 135 L1180 135')+path('M1055 370 L1180 370')+path('M240 350 C300 350 350 365 395 365'))+src(true)+users(true)+art('warehouse',445,48,145,'Warehouse','','warehouse-small')+label('Curated reporting tables',680,105,'hand blue',340)+art('lake',445,280,140,'Lake','','lake-small')+label('Retained source files',680,350,'hand teal',330),
'A hybrid can work well. Shared table data may reduce some copying and coordination.',
'This is a valid architecture. If duplicate curated data becomes expensive to coordinate, teams can consider shared tables over lake storage. Do not promise that all copies or pipelines disappear.');

add('06 / The lakehouse','Reliable tables over lake storage','The files stay in the lake. A table format defines committed versions.',
svg(outline(395,58,660,385,'teal')+path('M315 175 L393 175','em')+path('M245 350 C310 350 345 300 393 300','em')+path('M1060 175 L1180 175','em')+path('M1060 300 C1110 300 1145 365 1180 365','em'))+src(true)+users(true)+label('DATA LAKEHOUSE',490,25,'hand teal',550,'house-title')+art('house',465,130,225,'Lake storage','','house')+art('catalog',790,135,185,'Table format + catalog','','catalog')+note('Files + committed snapshots',465,405,'teal',550)+label('Iceberg is our course example',790,80,'muted',310),
'<strong>Lakehouse:</strong> lake storage with a reliable table layer for analytical workloads.',
'Parquet holds data. Iceberg defines metadata, snapshots and commits. A catalog resolves table identities and current metadata. Compatible engines read and write. Quality, access control, governance and optimization still need work. Raw text and images may remain alongside tables.',['lakehouse','iceberg']);

add('06 / Inside the lakehouse','Bronze, Silver, Gold','The same platform holds datasets at different levels of readiness.',
svg(outline(395,58,680,365,'teal')+path('M315 175 L412 175')+path('M245 350 C315 350 355 225 412 225')+path('M598 225 L631 225','em')+path('M818 225 L852 225','em')+path('M1078 180 L1180 180')+path('M820 420 C980 490 1110 400 1180 365'))+src(true)+users(true)+label('DATA LAKEHOUSE',505,28,'hand teal',500,'house-title')+box('Bronze','<p>Source history</p>',420,140,175,'bronze','bronze','Retain')+box('Silver','<p>Validated records</p>',640,140,175,'silver','silver','Clean')+box('Gold','<p>Business datasets</p>',860,140,175,'gold','gold','Serve')+note('Raw evidence',424,360,'orange',200)+note('Usable records',644,360,'',220)+note('Agreed meaning',867,360,'teal',230),
'Architecture describes the platform. Bronze, Silver and Gold describe its datasets.',
'Match the positions of the warehouse layer scene so learners can compare. Boundaries and names vary. Gold can contain a detailed fact table, not only aggregates. Bronze still has retention and access rules. The readiness concept can apply in other architectures.',['lakehouse']);

add('07 / One event through the layers','What changes between the stages?','Qafila receives the same delivered event twice.',
svg(path('M425 205 L499 205','em')+path('M925 205 L999 205','em'))+box('Bronze','<div class="data-title"><span>event_id</span><span>event</span></div><div class="data-row"><span>E7</span><span>delivered</span></div><div class="data-row"><span>E7</span><span>delivered</span></div>',30,75,380,'bronze','example-bronze')+box('Silver','<div class="data-title"><span>event_id</span><span>event</span></div><div class="data-row"><span>E7</span><span>delivered</span></div><div class="data-row repeated"><span>E7</span><span>duplicate</span></div>',530,75,380,'silver','example-silver')+box('Gold','<p style="font-size:21px">Delivered orders</p><div class="number">1</div><p style="font-size:20px">For this example</p>',1030,75,380,'gold','example-gold')+note('Keep both source copies',75,350,'orange',390)+note('One validated event',560,350,'',370)+note('Apply the metric definition',1020,350,'teal',410),
'<strong>A different promise:</strong> retained evidence, validated records, business meaning.',
'Minimal illustrative data: one delivered event belongs to one order and there are no other events. Bronze retains two copies. Silver deduplicates by event identity. Gold counts distinct delivered orders within the agreed reporting window. A full model handles city mapping, dates, cancellations and late arrivals. Transactions do not automatically deduplicate events.');

add('08 / The comparison','Three organizing ideas','The workload and required guarantees guide the choice.',
'<div class="comparison"><div class="compare-col"><div class="art warehouse" aria-hidden="true"></div><h3>Warehouse</h3><p>Shared analytical<br>tables and SQL models</p><p class="fine">Qafila’s financial reporting</p></div><div class="compare-col"><div class="art lake" aria-hidden="true"></div><h3>Lake</h3><p>A repository of files<br>for retention and exploration</p><p class="fine">Courier event history</p></div><div class="compare-col"><div class="art house" aria-hidden="true"></div><h3>Lakehouse</h3><p>Reliable tables<br>over lake storage</p><p class="fine">Shared datasets for BI and ML</p></div></div>',
'Real products combine capabilities. None of these names guarantees clean data.',
'Ask learners to explain each pattern without a vendor name. These are organizing ideas and typical uses, not exclusive capabilities. Warehouses support external tables and semi-structured data. Lakes can be governed. Cost and performance depend on workload and operations.',['aws','lakehouse']);

add('09 / A learner decision','What should Qafila change first?','The dashboard reads a mixture of old and new Parquet files.',
art('analyst',1100,70,270,'“Which result can I trust?”','','quiz-analyst')+'<button class="quiz-option" data-answer="A" style="top:35px"><span class="letter">A</span><span>Rename the folder “Gold”</span></button><button class="quiz-option" data-answer="B" style="top:140px"><span class="letter">B</span><span>Use atomic table commits and compatible readers</span></button><button class="quiz-option" data-answer="C" style="top:245px"><span class="letter">C</span><span>Move the same files to a bigger server</span></button><p class="feedback" role="status"></p><p class="quiz-instruction">Choose an answer, then explain what guarantee it changes.</p>',
'Explain what changes for the reader, as well as the technology you would choose.',
'Allow 45 seconds. B is correct. Ask what A and C fail to change. Follow up: does the table format also remove duplicate E7 events? No, that requires a data transformation rule.',['iceberg']);

add('09 / The guarantee','One complete table version','A writer prepares new files, then publishes a snapshot atomically.',
svg(path('M410 210 L565 210','em')+path('M870 210 L1025 210','em'))+box('Snapshot 41','<div class="number">41</div><p>Both old files</p>',40,100,360,'silver','snapshot41')+art('engineer',600,85,210,'Prepare + commit','','commit-engineer')+box('Snapshot 42','<div class="number">42</div><p>Both new files</p>',1050,100,350,'gold','snapshot42')+note('Current reads remain consistent',40,380,'',440)+note('New reads use the new snapshot',1000,380,'teal',440),
'<strong>Atomic publication:</strong> readers use a committed version while a writer works.',
'Snapshot numbers are illustrative. Existing queries can continue using their chosen snapshot. Failure before commit leaves the current table version visible. Maintenance later removes unreferenced files. These guarantees do not replace access control, business definitions or quality checks.',['iceberg']);

add('10 / Further reading','The architecture responds to a need','Qafila is fictional. The design questions apply to real data platforms.',
`<div class="references"><div><h3>The questions to take away</h3><p>What evidence must we retain?</p><p>Who needs to query it?</p><p>What does each dataset promise?</p><p class="note">A warehouse, lake, or lakehouse can fit.<br>Choose with the workload and the team.</p></div><div><h3>Sources and continuation</h3><div class="reference-list"><a href="${sources.reference}" target="_blank" rel="noopener">Data with Baraa · Visual reference</a><a href="${sources.aws}" target="_blank" rel="noopener">AWS · Warehouse and lake comparison</a><a href="${sources.lakehouse}" target="_blank" rel="noopener">Databricks · Lakehouse and data layers</a><a href="${sources.iceberg}" target="_blank" rel="noopener">Apache Iceberg · Reliable table updates</a><a href="day1.html#12">Continue Day 1 · ETL and ELT</a></div></div></div>`,
'The next lab turns these promises into data transformations and checks.',
'The video provides the visual teaching reference: hand-drawn architecture, stable sources and consumers, progressive introduction of platform layers. The illustrations and Qafila narrative are original. Text and diagram layout remain editable in the source.');

scenes.forEach((s,i)=>{
 const el=document.createElement('section');el.className='scene';el.dataset.slide=String(i+1);el.setAttribute('aria-label',`${i+1}. ${s.title}`);
const heading=s.cover?'<div class="cover-title"><p class="eyebrow">MODERN DATA ENGINEERING</p><h1>Warehouse,<br>lake,<br><span class="teal">lakehouse</span></h1><p class="lead">One company.<br>A changing architecture.</p></div>':`<div class="heading"><h2>${s.title}</h2><p class="subtitle">${s.subtitle}</p></div>`;
 if(s.cover)s.body=s.body.replace('viewBox="0 0 1440 510"','viewBox="0 0 730 485"');
 el.innerHTML=`<header><div class="brand"><img src="../assets/img/academy.svg" alt="SDAIA Academy"><span>QAFILA / DATA PLATFORM STORY</span></div><span class="chapter">${s.chapter}</span></header>${heading}<div class="board ${s.cover?'cover-board':''}">${s.body}</div><p class="takeaway"><span class="signal" aria-hidden="true"></span><span>${s.takeaway}</span></p><footer><span>SDAIA Academy · Modern Data Engineering</span><span>${String(i+1).padStart(2,'0')} / ${scenes.length}</span></footer>`;root.appendChild(el);
});
const els=[...root.querySelectorAll('.scene')],notes=document.getElementById('notes-panel');let index=-1,overview=false;
const progress=document.createElement('div');progress.id='progress';root.appendChild(progress);
function fit(){if(!overview)root.style.transform=`translate(-50%,-50%) scale(${Math.min(innerWidth/1600,innerHeight/900)})`;}
function show(n,update=true){
 const next=Math.max(0,Math.min(els.length-1,n)),oldIds=new Set(index<0?[]:[...els[index].querySelectorAll('[data-id]')].map(e=>e.dataset.id));index=next;
 els.forEach((e,j)=>{e.classList.toggle('active',j===index);e.setAttribute('aria-hidden',String(j!==index));e.inert=j!==index;});
 els[index].querySelectorAll('[data-id]').forEach(e=>e.classList.toggle('new',!oldIds.has(e.dataset.id)));
 if(update)history.replaceState(null,'',`#${index+1}`);progress.style.width=`${(index+1)/els.length*100}%`;
 document.getElementById('previous').disabled=index===0;document.getElementById('next').disabled=index===els.length-1;
 notes.innerHTML=`<h3>${index+1}. ${scenes[index].title}</h3><p>${scenes[index].notes}</p><p style="font-size:16px;margin-top:18px">${scenes[index].refs.map(k=>`<a href="${sources[k]}" target="_blank" rel="noopener">${k}</a>`).join(' · ')}</p>`;
 document.getElementById('live-status').textContent=`Slide ${index+1} of ${els.length}: ${scenes[index].title}`;
}
function toggleNotes(){document.getElementById('notes').setAttribute('aria-expanded',String(notes.classList.toggle('open')));}
function toggleOverview(){overview=!overview;document.body.classList.toggle('overview',overview);if(overview){root.style.transform='';els.forEach(e=>{e.inert=false;e.setAttribute('aria-hidden','false');});}else{show(index);fit();}}
function fullscreen(){if(document.fullscreenElement)document.exitFullscreen();else document.documentElement.requestFullscreen().catch(()=>{});}
document.getElementById('previous').onclick=()=>show(index-1);document.getElementById('next').onclick=()=>show(index+1);document.getElementById('notes').onclick=toggleNotes;document.getElementById('overview').onclick=toggleOverview;document.getElementById('fullscreen').onclick=fullscreen;
els.forEach((e,i)=>e.addEventListener('click',()=>{if(overview){toggleOverview();show(i);}}));
document.querySelectorAll('.quiz-option').forEach(b=>b.addEventListener('click',()=>{const p=b.closest('.scene');p.querySelectorAll('.quiz-option').forEach(x=>x.classList.remove('correct','wrong'));const correct=b.dataset.answer==='B';b.classList.add(correct?'correct':'wrong');p.querySelector('.feedback').textContent=correct?'Yes. Readers use one committed table version.':'That does not make a multi-file update atomic. Try again.';}));
document.addEventListener('keydown',e=>{if(e.ctrlKey||e.metaKey||e.altKey)return;if(e.target.closest('button')&&['Enter',' '].includes(e.key))return;if(['ArrowRight','ArrowDown','PageDown',' '].includes(e.key)){e.preventDefault();show(index+1);}else if(['ArrowLeft','ArrowUp','PageUp'].includes(e.key)){e.preventDefault();show(index-1);}else if(e.key==='Home'){e.preventDefault();show(0);}else if(e.key==='End'){e.preventDefault();show(els.length-1);}else if(e.key.toLowerCase()==='n')toggleNotes();else if(e.key.toLowerCase()==='o')toggleOverview();else if(e.key.toLowerCase()==='f')fullscreen();else if(e.key==='Escape'){if(overview)toggleOverview();notes.classList.remove('open');document.getElementById('notes').setAttribute('aria-expanded','false');}});
let touchX=null;document.addEventListener('touchstart',e=>{touchX=e.touches[0].clientX;},{passive:true});document.addEventListener('touchend',e=>{if(touchX!==null){const dx=e.changedTouches[0].clientX-touchX;if(Math.abs(dx)>65)show(index+(dx<0?1:-1));touchX=null;}},{passive:true});
window.addEventListener('resize',fit);window.addEventListener('hashchange',()=>show((parseInt(location.hash.slice(1),10)||1)-1,false));fit();show((parseInt(location.hash.slice(1),10)||1)-1);
if(location.search.includes('check')){const initial=index,issues=[];for(let i=0;i<els.length;i++){show(i,false);const r=els[i].getBoundingClientRect();for(const e of els[i].querySelectorAll('.figure,.annotation,.label,.stage-box,.heading,.takeaway,.compare-col,.quiz-option,.feedback,.quiz-instruction,.references')){const b=e.getBoundingClientRect();if(!b.width||!b.height)continue;if(b.left<r.left-1||b.right>r.right+1||b.top<r.top-1||b.bottom>r.bottom+1)issues.push(`Slide ${i+1}: ${e.className} outside canvas`);if(e.scrollWidth>e.clientWidth+3)issues.push(`Slide ${i+1}: ${e.className} overflow`);}}show(initial,false);window.STORY_LAYOUT_CHECK={slides:els.length,issues};const report=document.createElement('output');report.id='check-report';report.style.cssText='position:fixed;left:8px;bottom:8px;font:12px monospace;z-index:99;background:white;padding:5px';report.textContent=`CHECK ${els.length} slides: ${issues.length?issues.join('; '):'OK'}`;document.body.appendChild(report);}
})();
