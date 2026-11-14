#include <iostream>
#include <queue>
#include <vector>
#include <thread>
#include <mutex>
#include <semaphore>
#include <chrono>
#include <string>
using namespace std;


// =====================================================================
//  SECTION 1: SHARED DATA
// =====================================================================
/*
   Ei section e shob data rakhsi ja shob thread eksathe share kore.
   Producer, consumer, main thread, shobai ei gulo dekhe ba bodlay.

   Ekhane amar shobcheye boro concern chilo: dui thread jodi eksathe
   buffer e push ar pop kore, tahole queue er bhitorer pointer gulo
   ulta-palta hoye jabe. Task harabe, ba ekta task duibar ber hobe,
   ba program crash korbe. Eitai race condition. Tai ekta mutex 'm'
   rakhsi. Niyom ekta: buffer ba kono counter touch korar age 'm' er
   lock nite hobe. Lock jar kache, shudhu shei touch korte parbe.

   Kintu shudhu mutex diye "buffer full hole producer thamo" ba
   "buffer khali hole consumer thamo" kora jay na. Tai duita semaphore:
   emptySlots gone koyta khali jaiga ase, fullSlots gone koyta task ase.
   Semaphore er value 0 hole acquire() kora thread oikhanei ghumiye
   thake, jotokkhon na keu release() kore. Semaphore nijei thread-safe,
   tai oder jonno alada lock lage na. 1000 likhsi karon C++ e semaphore
   er ekta max value bole dite hoy, amader capacity er cheye boro hok.

   Counter gulo (created, processed, completed, seen) task accounting
   er jonno. Shesh e mile dekhbo protita task thik ekbar complete hoise
   kina. seen[id] = 0 mane task harise, 2+ mane duplicate hoise.
*/
queue<int> buffer;
mutex m;
counting_semaphore<1000> emptySlots(0);
counting_semaphore<1000> fullSlots(0);

int created, processed, completed;
int fullWaits, emptyWaits;
vector<int> seen;


// =====================================================================
//  SECTION 2: TIME HELPERS
// =====================================================================
/*
   chrono library ta shudhu ei duita function e use korsi, ar kothao na.
   Ami eta baad dite chaisilam, kintu experiment e "tasks per second"
   mapte hobe, ar wall-clock time mapar standard upay eitai. clock()
   use korle CPU time ashto, ja ghumanto thread er time gone na, tai
   bhul result dito.

   doWork() asol kaj er bodole thread ke kichu microsecond ghum parai.
   Er mane ekta task process korte kichu shomoy lage, jemon real job
   service e file likha ba network call e lage.
*/
double nowSeconds() {
    return chrono::duration<double>(chrono::steady_clock::now().time_since_epoch()).count();
}

void doWork(int microseconds) {
    if (microseconds > 0) this_thread::sleep_for(chrono::microseconds(microseconds));
}


// =====================================================================
//  SECTION 3: PRODUCER
// =====================================================================
/*
   Producer er kaj: n ta task banano ar buffer e rakha. Protita task er
   number unique (id * n + i), jate shesh e gune dekhte pari kon task
   harise ba duibar hoise.

   Rakhar age 3 ta dhap, ar ordering ta khub important:
     1. emptySlots.acquire()  -> khali jaiga na thakle ekhanei ghumai.
     2. lock niye push        -> buffer e dhokai.
     3. fullSlots.release()   -> consumer ke janai notun task ase.

   Ami jodi age lock nitam, tarpor acquire() kortam, tahole bipod hoto:
   buffer full, producer lock dhore ghumiye ase, consumer lock pabe na
   tai kichu ber korte parbe na, keu kono din jagbe na. Eitai deadlock.
   Tai semaphore e wait kori lock er BAIRE, lock nei shudhu push er jonno.
*/
void producer(int id, int n, bool show) {
    for (int i = 0; i < n; i++) {
        int task = id * n + i;

        /*
           try_acquire() kono wait kore na, shudhu dekhe jaiga ase kina.
           Fail korle bujhi buffer full, tai fullWaits barai (eta test er
           proman er jonno), tarpor asol acquire() diye ghumai. Logic er
           jonno shudhu acquire() i jothesto, try_acquire() ta shudhu
           wait gonar jonno. Counter barate o lock nisi, karon eta o shared.
        */
        if (!emptySlots.try_acquire()) {
            { lock_guard<mutex> lock(m); fullWaits++; }
            emptySlots.acquire();
        }

        /*
           lock_guard { } block e dhuklei lock ney, block shesh holei nijei
           chere dey. Tai unlock bhule jawar ba majhe return kore lock atke
           thakar bhoy nai. Print o lock er bhitore, nahole duita thread er
           line ek line e mishe jeto. Lock ber hoyei fullSlots.release(),
           jate kono ghumanto consumer jege task nite pare.
        */
        {
            lock_guard<mutex> lock(m);
            buffer.push(task);
            created++;
            if (show) cout << "Producer " << id << " created task " << task << endl;
        }
        fullSlots.release();
    }
}


// =====================================================================
//  SECTION 4: CONSUMER
// =====================================================================
/*
   Consumer er kaj: buffer theke task tule neya ar process kora, jotokkhon
   na STOP signal (-1) pay. Producer er ulta mirror:
     1. fullSlots.acquire()   -> task na thakle ekhanei ghumai.
     2. lock niye pop         -> buffer theke ber kori.
     3. emptySlots.release()  -> producer ke janai ekta jaiga khali holo.

   Ekhaneo ekoi concern: semaphore e wait kori lock er baire, nahole
   deadlock. Ar fullSlots.acquire() pass korle nishchit buffer e ontoto
   ekta item ase, tai front() kokhono khali queue theke porbe na.
   Eitai "consumer unavailable item remove korbe na" requirement ta
   puron kore.
*/
void consumer(int id, int work, bool show) {
    while (true) {
        if (!fullSlots.try_acquire()) {
            { lock_guard<mutex> lock(m); emptyWaits++; }
            fullSlots.acquire();
        }

        /*
           Lock er bhitore shudhu front + pop + counter. Ekhane chesta
           korsi lock ta jotota pari choto rakhte. task variable ta lock
           er baire declare korsi, jate lock chere deyar poro value ta
           hate thake. STOP hole processed gunbo na, karon STOP asol task
           na, shudhu bondho howar signal.
        */
        int task;
        {
            lock_guard<mutex> lock(m);
            task = buffer.front();
            buffer.pop();
            if (task != -1) processed++;
            if (show && task != -1) cout << "   Consumer " << id << " took task " << task << endl;
        }
        emptySlots.release();

        /*
           STOP pele loop theke ber hoye jai. Na hole kaj kori, ar ei kaj
           ta kori lock CHARA. Eta ichchha kore kora: kaj korte 200
           microsecond lagle ar lock dhore rakhle, oi shomoy onno shob
           producer ar consumer boshe thakto, parallel kaj er kono labh
           hoto na. Kaj shesh e abar lock niye seen[] ar completed barai,
           karon egulo o shared data.
        */
        if (task == -1) break;
        doWork(work);
        {
            lock_guard<mutex> lock(m);
            seen[task]++;
            completed++;
        }
    }
}


// =====================================================================
//  SECTION 5: RUN ONE EXPERIMENT
// =====================================================================
/*
   Ei function ta ekta full experiment chalay: reset, thread start,
   shutdown, ar shesh e hishab mile PASS/FAIL dekhay. Protita test ar
   experiment ei ekta function diyei chole, shudhu number bodlay.

   P = producer, C = consumer, cap = buffer capacity,
   n = protita producer koyta task banabe, work = protita task er kaj
   (microsecond), show = protita step print korbo kina.
*/
void run(string name, int P, int C, int cap, int n, int work, bool show) {
    int total = P * n;

    /*
       Reset: ager run er kono kichu jeno theke na jay. Semaphore er value
       sorasori set kora jay na, tai try_acquire() diye 0 porjonto namai,
       tarpor emptySlots e 'cap' ta jaiga release kori. Ei line tai buffer
       ke "bounded" banay: emptySlots er value kokhono cap er beshi hobe
       na, tai buffer e kokhono cap er beshi item dhukbe na.
    */
    while (!buffer.empty()) buffer.pop();
    while (emptySlots.try_acquire()) {}
    while (fullSlots.try_acquire()) {}
    emptySlots.release(cap);
    created = processed = completed = fullWaits = emptyWaits = 0;
    seen.assign(total, 0);

    double start = nowSeconds();

    /*
       Consumer age start korsi, tarpor producer. Ichchha kore: consumer
       ra age theke khali buffer e ghumiye thakbe, tai empty-buffer
       condition ta shotti test hoy. Tarpor shob producer er join() e
       wait kori, mane shob task buffer e dhuke gese.
    */
    vector<thread> prod, cons;
    for (int i = 0; i < C; i++) cons.emplace_back(consumer, i, work, show);
    for (int i = 0; i < P; i++) prod.emplace_back(producer, i, n, show);
    for (auto& t : prod) t.join();

    /*
       Shutdown: ekhane amar concern chilo, consumer ra kibhabe janbe je
       ar task ashbe na? Na janle tara fullSlots.acquire() e chirokal
       ghumiye thakbe, ar join() kokhono shesh hobe na. Tai protita
       consumer er jonno ekta STOP (-1) dhokai, producer er motoi niyom
       mene (jaiga na thakle wait). Queue FIFO, tai STOP gulo shob real
       task er PORE ase. Mane STOP pawar age shob real task ber hoye
       jabe, kono task harabe na. Protita consumer thik ekta STOP pay.
    */
    for (int i = 0; i < C; i++) {
        emptySlots.acquire();
        { lock_guard<mutex> lock(m); buffer.push(-1); }
        fullSlots.release();
    }
    for (auto& t : cons) t.join();

    double sec = nowSeconds() - start;

    /*
       Verification: protita task id er seen[] dekhi. 0 mane lost, 1 er
       beshi mane duplicate. Shob counter expected er shathe mile, kono
       lost/duplicate nai, ar buffer shesh e khali, tahole PASS. Ekhane
       lock lage na, karon shob thread already join hoye gese, ar keu
       data touch korche na.
    */
    int duplicate = 0, lost = 0;
    for (int i = 0; i < total; i++) {
        if (seen[i] > 1) duplicate++;
        if (seen[i] == 0) lost++;
    }
    bool ok = created == total && processed == total && completed == total
              && duplicate == 0 && lost == 0 && buffer.empty();

    cout << "\n[" << name << "]  P=" << P << " C=" << C << " capacity=" << cap << endl;
    cout << "  Created=" << created << "  Processed=" << processed
         << "  Completed=" << completed << "  (expected " << total << ")" << endl;
    cout << "  Duplicate=" << duplicate << "  Lost=" << lost << endl;
    cout << "  Producer waits (full)=" << fullWaits
         << "  Consumer waits (empty)=" << emptyWaits << endl;
    cout << "  Time=" << sec << " sec   Speed=" << (int)(completed / sec) << " tasks/sec" << endl;
    cout << "  RESULT: " << (ok ? "PASS" : "FAIL") << endl;
}


// =====================================================================
//  SECTION 6: WRONG WAY (NO MUTEX) DEMO
// =====================================================================
/*
   Assignment e bola ase ekta bhul synchronization er failure dekhate.
   Tai 4 ta thread eksathe ekta counter 3000000 bar barabe, ekbar lock
   chara, ekbar lock diye.

   "count = count + 1" dekhte ek line, kintu CPU te 3 ta dhap: read,
   +1, write. Dui thread eksathe read korle duijonei same value pay,
   duijonei +1 kore likhe dey. Fole duita increment er jaygay ekta hoy,
   ekta update harise. Lock chara result 12000000 er cheye kom ashe.

   volatile dilam jate compiler pura loop ke "count += 3000000" e
   bodle na fele, tahole race dekha jeto na. volatile race thik kore na,
   shudhu race ta dekha jabe sheta nishchit kore. Lock wala version e
   result shob shomoy thik 12000000.
*/
volatile int wrongCount = 0;
int rightCount = 0;
mutex countLock;

void wrongWorker() {
    for (int i = 0; i < 3000000; i++) wrongCount = wrongCount + 1;
}

void rightWorker() {
    for (int i = 0; i < 3000000; i++) {
        lock_guard<mutex> lock(countLock);
        rightCount = rightCount + 1;
    }
}

void wrongWayDemo() {
    wrongCount = 0;
    rightCount = 0;
    vector<thread> w, r;
    for (int i = 0; i < 4; i++) w.emplace_back(wrongWorker);
    for (auto& t : w) t.join();
    for (int i = 0; i < 4; i++) r.emplace_back(rightWorker);
    for (auto& t : r) t.join();

    cout << "\n4 threads x 3000000 = expected 12000000" << endl;
    cout << "Without mutex: " << wrongCount << endl;
    if (wrongCount != 12000000) cout << "   <-- updates were LOST (race condition)" << endl;
    else cout << "   <-- no loss this time, run option 2 again" << endl;
    cout << "With mutex   : " << rightCount << endl;
}


// =====================================================================
//  SECTION 7: TESTS AND EXPERIMENTS
// =====================================================================
/*
   Tests: protita test ekta nirdishto condition ke jor kore ghotay.
     - FULL buffer: capacity 1, ekta slow consumer. Producer ra bar bar
       full buffer e atkabe, tai "Producer waits" onek beshi hobe.
     - EMPTY buffer: 1 producer, 4 consumer. Consumer ra beshi, tai
       khali buffer e wait korbe.
     - HIGH contention: 8 + 8 thread, capacity 1, kaj 0. Shobai ekoi
       lock er jonno maramari korbe. Eta pass korle bujhi chap er moddheo
       kono task harasse na.

   Experiments: protita task 200 microsecond.
     - Exp 1: consumer 1, 4, 8. Kaj lock chara hoy, tai consumer barale
       kaj parallel e hoy, speed barar kotha.
     - Exp 2: capacity 1, 10, 100. Capacity barale producer kom atkay,
       kintu consumer jodi bottleneck hoy, speed tematha barbe na.
       Eta report e explain korbo.
     - Exp 3: producer 1, 4, 8 (consumer 4 fixed). Assignment e producer
       sonkha o bodlate bolse. Consumer ra bottleneck hole producer
       barale speed barbe na, shudhu producer der wait barbe.
*/
void runTests() {
    run("Test FULL buffer  (capacity 1, slow consumer)", 3, 1, 1, 20, 500, false);
    run("Test EMPTY buffer (1 producer, 4 consumers)", 1, 4, 5, 20, 0, false);
    run("Test HIGH contention", 8, 8, 1, 1000, 0, false);
}

void runExperiments() {
    cout << "\n(each task = 200 microseconds)" << endl;
    run("Exp 1 consumers=1", 4, 1, 10, 300, 200, false);
    run("Exp 1 consumers=4", 4, 4, 10, 300, 200, false);
    run("Exp 1 consumers=8", 4, 8, 10, 300, 200, false);
    run("Exp 2 capacity=1",   4, 4, 1,   300, 200, false);
    run("Exp 2 capacity=10",  4, 4, 10,  300, 200, false);
    run("Exp 2 capacity=100", 4, 4, 100, 300, 200, false);
    run("Exp 3 producers=1",  1, 4, 10,  300, 200, false);
    run("Exp 3 producers=4",  4, 4, 10,  300, 200, false);
    run("Exp 3 producers=8",  8, 4, 10,  300, 200, false);
}


// =====================================================================
//  SECTION 8: MAIN MENU
// =====================================================================
/*
   Live demo r oral defense er jonno menu rakhsi, jate teacher je part
   dekhte chay sheta alada kore chalate pari. Option 1 choto run, protita
   step print kore, jate chokhe dekha jay producer ar consumer kibhabe
   palakrome kaj korche. Number chara onno kichu dile cin fail kore, tokhon
   program bondho kori, nahole menu infinite loop e ghurte thakto.
*/
int main() {
    int choice = -1;
    while (choice != 0) {
        cout << "\n===== MENU =====" << endl;
        cout << "1. Normal run (with logs)" << endl;
        cout << "2. Wrong way (no mutex) demo" << endl;
        cout << "3. Tests (full, empty, high contention)" << endl;
        cout << "4. Experiments" << endl;
        cout << "0. Exit" << endl;
        cout << "Enter choice: ";
        if (!(cin >> choice)) break;

        if (choice == 1) run("Normal run", 3, 2, 3, 4, 1000, true);
        else if (choice == 2) wrongWayDemo();
        else if (choice == 3) runTests();
        else if (choice == 4) runExperiments();
    }
    return 0;
}
