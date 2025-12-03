- **Title**:Keep It Simple: Testing Databases via Differential Query Plans
    - **Author**:JINSHENG BA、MANUEL RIGGER
    - **Contexts**:
    1. Introduction
	    - TQS用于发现连接优化中逻辑错误，将给定的表拆为多个子表，并用给定的表来验证子表的查询结果。而TQS找到的15个bug中有14个都因为同一查询采用不同的查询计划产生差异而发现（对于这部分而言，不需要ground truth）
	    - 因此提出了DQP(differential query plan)，对相同查询强制执行不同的查询计划，并验证结果是否一致
	    - 给定数据库D及查询Q，使用不同的两种查询计划P和P'，都在D中执行Q，若Q(P,D)!=Q(P',D)，则有bug
	2. TQS Study
		- TQS需要实现多个图形和表格作为内部组件来导出ground truth
		- 因此对TQS进行研究，探究这些bug是否可以通过更简单的方法知道
			- RQ1:TQS发现的bug有多少与连接优化相关
			- RQ2:TQS是如何报告这些bug的，如何证明是DBMS而非TQS出了问题
		- 由于TQS源码不公开，因此只选取TQS公开的bug列表作为研究目标。其中只包括Mysql、MariaDB，TiDB的报告（PolarDB未出现在报告中）
		- TQS论文生成报告了所有发现的92个bug，但是实际上仅报告了21个。经判断，TQS论文提及92个可能指的是bug的测试用例数量。
		- 公开bug列表中每一个bug报告都对应TQS论文中的bug类型而非一个bug，且有3个bug报告没有匹配到bug类型，可能是在提交TQS论文后才提交的
		- 对于20个bug报告，其中仅15个是unique的
		- RQ1:15个unique bug报告中，10个与join优化有关，其余5个的测试用例至少有一个SUBQUERY子句
		- RQ2:所有10个join相关bug及另4个bug都通过演示不同查询计划的相同查询得到了不同结果；剩余那个bug通过插入NULL行后返回更少行来表明bug，这源于SUBQUERY优化错误
		- 因此，大部分bug可通过检查不同计划的相同查询到不一致结果来发现
	3. Approach
		1. 数据库生成：各种方法都行，可采用基于突变或者规则等方法，假设随机
		2. 查询生成：各种方法都行，假设随机
		3. DQP执行查询Q时，DBMS会自动化生成计划P，然后DQP尝试强制DBMS派生出另一个查询计划P‘，查询提示与系统变量均可，仅通过SQL关键字而非DBMS源码来影响计划
		4. 比较结果一致性
	4. Implementation
		- 在SQLancer中实现了DQP，采用grammar-based方法，随机生成SQL语句
		- 生成D与Q时，DQP都随机遍历语法树生成对应的SQL语句
		- 更新SQLancer使其支持为Mysql与MariaDB生成join子句
		- 派生计划
			- Query hints，注释
			- System variable，配置
		- 为了效率，在迭代中枚举所有query hint与system variable的可能值来强制执行多个计划。这是可行的，因为值有限且少
		- 有些错误报错源于有歧义的查询，其不能保证一致或可预测的查询。为排除他们，可以通过检查表中行顺序的不同是否影响结果来判断。迭代时，DQP默认重新生成Q，固定迭代次数后重新生成D
		- 歧义查询检测算法中，为减少复杂度，用C-reduce及手动最小化测试用例
	5. Evaluation
		- Q1：bug reduction，可否找到TQS找到的bug
		- Q2：New bugs，可否找到未知错误
		- Q3：bug-findings efficiency，找bug效率
		- Q4：bug-findings effectiveness，比其他技术，DQP找bug有效性
		- Q5：coverage，覆盖率
		- 测试平台包括Mysql、MariaDB、TiDB
		- Q1：bug reduction
			- TQS找到的15个bug，DQP能找到14个，其中包含所有的10个join相关bug
		- Q2：new bugs
			- 找到了26个新bug，其中21个是逻辑错误，其中15个join related
		- Q3：bug-finding efficiency
			- 由于TPS未开源代码且部分实验配置未知，故不易比较
			- SQLancer+DQP在24h中共找到216个bug测试用例
		- Q4：bug-finding effectiveness
			- 将DQP与NoREC/TLP比较，PQS不受对应DBMS支持
			- 采用与先前人一样的方法论
			- DQP找到21个逻辑错误未被NoREC/TLP发现
			- DQP无法重新NoREC找到了所有4个bug及TLP找到的36个中的31个bug
		- Q5：coverage
			- 检查查询计划覆盖率：执行计划/所有计划
			- 通过提示与变量覆盖率及join覆盖率评估其对优化器的影响度，还评估了代码覆盖率
			- plan coverage，为去除不稳定的辅助信息，在plan中去除了schema name（行列名）、estimated cost（基数）及random identifiers（行标识符等）。且上限无限大，故在24h及10次运行中组合DQP、NoREC及TLP涵盖的所有unique计划来估计上限
			- 无法比较DQP与TLP的计划覆盖率，TLP源码未公开
			- Hint 与 variable coverage
				- 可能受提示/系统变量影响的主要三类：join（算法与顺序）、Index（算法及适用范围）、Table（读写表的策略）、
			- Join coverage
				- SQLancer+DQP涵盖了Mysql与MariaDB的12个join运算符中的7个及TiDB的全部3个运算符
			- Code Coverage
				- 由于TQS源码不可用，无法比较DQP与TQS的代码覆盖率
				- 由于DQP在连接优化中发生错误，只测了查询优化的代码覆盖率
				- 结果表明SQLancer+DQP覆盖了Mysql 22.2%，MariaDB 27.7%，TiDB 36.1%
				- DQP在24h内覆盖了数千个unique的查询计划及超一半的mysql、mariadb与tidb的连接运算符
    - **Comment**:
	    - 这篇论文的动机我没看懂。他是说由于TQS太复杂，有些bug不需要使用ground truth，只用差异比较就能找到。但是TQS当初提出来的时候就是为了解决差异测试存在两边同时出错的问题，现在怎么又改回去了。
	    - 并且他很多实验的细节也没有细讲，反而是很多篇幅放在指出TQS这篇论文的问题上。
- **Title**:Fawkes: Finding Data Durability Bugs in DBMSs via Recovered Data State Verification
    - **Author**:Zhiyong Wu
    - **Contexts**:
	- DMBS在处理数据持久性时可能出现问题，即Data Durability Bugs（DDB）
	- 文章分析了四种DBMS中的43个DDB并提出了Fawkes，用于检测具有恢复数据状态验证的分布式数据库
	1. Introduction
		- 目前缺乏检测DDB的有效方法，手动模型分析耗时耗力
		- 传统的故障注入工具（Jespen，Mallory）常用于通过模拟网络分区、节点崩溃和延迟来评估分布式系统的容错性和一致性。但存在局限：随机注入故障导致缺乏与特定状态相关的持久性问题，且旨在验证分布式节点的正确性，无法解决单节点恢复后的数据丢失、数据不一致及日志损坏问题
		- 分析了PostgreSQL、Mysql、IoTDB、TDengine的43个DDB
		- 发现
			- Manifestations：DDB主要四个表现，数据丢失、数据不一致、日志损坏、系统不可用
			- Root Cause：大多源于有缺陷的崩溃恢复及数据刷新逻辑
			- Triggering conditions：步骤workload generation->checkpoint execution->fault-induced crash->revealing anomalies after recovery。且大部分源自执行sql语句相关的文件系统或内核级调用操作时故障
		- 技术挑战
			- 许多问题需要针对性注入故障才能发现
			- 要系统性探索DBMS中很少执行的关键的路径
			- 要准确确定DBMS是否已经正确恢复
	2. Motivation Study
		- Finding1：Observed Manifestation
			- 主要DDB有四种，data loss、data inconsistency、log corruption、system unavailability
		- Finding2：DDB Severity
			- 81%DDB会导致数据安全问题并被标记为关键
		- Finding3：Root Cause
			- 72%的数据持久性错误源于有问题的崩溃回复与数据刷新
		- Finding4：Trigger Steps
			- DDB可由4步序列稳定触发
			- Initial Data Creation and Continuous Data Modification -> checkpoint execution -> Fault-Induced Crash -> System recovery and Replay
		- Finding5：Fault Categories
			- 在43个DDB中，可确定7个不同类别导致在第三步中导致system crash
		- Finding6：DBMS behaviors during fault
			- 在第三步出现bug时，86%的DDB的SQL语句都在执行文件系统调用或其他内核级系统调用中
		- Finding7：Involved SQL grammar
			- DDB影响不同的SQL语法特征，在43个bug中跨越了37个不同的功能
		- 现有方法
			- Manually crafted testing，劳动密集型，难以扩展，不易找到隐藏DDB
			- automated fault injection testing tools，无法精确实现注入，覆盖率不足，正确性检查粗粒度，无法识别数据丢失或不一致
	3. Design of FAWKES
		- 故障检测部分：DBMS先由Context-Aware Fault Injector扫描源码（系统或者内核调用部分）进行故障检测；然后利用库注入方法，将fault注入检测窗口
		- 测试部分：随机生成大量数据与查询；收集fault-injection site由semantic-guided fault trigger决定在哪个site中触发；然后执行sql序列；一旦DBMS crash自动reboot并恢复，Data State Verifier将恢复后状态与预期比较
		- Context-Aware Fault Injection
			- Fawkes扫描源码，确定critical code segments（即fault injection site），在其中disruption最有可能影响数据持久性
			- 具体而言，在编译阶段执行上下文分析，以在DBMS函数间简历调用依赖关系图，当出现OS基础库调用链时，相关代码、函数会被标记为fault injection site
			- Fault Location bitmap：用于记录site位置，用于追踪覆盖率
		- Funtionality-Guided Fault Triggering
			- F={f1,f2,f3....,fl}为所有检测到的fault injection，需要从中选一个触发
			- 随机选择，已丢失不常用路径；暴力选择，太大的l难以处理；coverage-guided专注于一般代码覆盖而非与site相关的执行路径；deep-priority仅增强单个执行路径上的site覆盖，忽略了全面的site覆盖
			- 故提出了Functionality-guided策略
				- 维护一个fault-functionality table（在fault location bitmap中）用于记录源文件与相关sql功能间的关系
				- 测试时，通过bitmap监控site的覆盖率，若site未被完全出发，则查阅fault-functionality table来识别相关的sql功能，引导后续查询
				- Fawkes会维护FilePool，其中包含未覆盖site最多的文件
		- Checkpoint-based data graph verification
			- 数据丢失与不一致的问题，不易检测，要比较崩溃前后的状态
			- 利用data state verifier，引入checkpoint-based data graph verification，构建一个数据图用于捕获崩溃前的DBMS元数据状态而非全部数据。当出现注入的bug从而恢复后，先检查恢复日志或错误信息以检测系统不可用和日志损坏问题，然后分析恢复的DBMS日志以定位最新检查点，并根据检查点信息更新数据图，最后将恢复的数据与更新数据图中的约束比较以检测数据丢失或不一致
			- Graph rectification
				- 若functional-guided fault触发时，会更新graph，但恢复不会恢复未提交事务
				- 故要纠正graph使其与恢复的状态保持一致
				- 数据不一致有2种
					- 元数据不一致，由graph匹配检测
					- 实体数据不一致，fawkes会跟踪最后一个checkpoint后提交的所有DML语句，验证恢复的数据中是否有相应行。
	4. Implementation
		- Workload generator
			- 对于数据，创建不同的表，每个表有10-100个随机列，后随机应用索引与外键，最后填充数据
			- 对于查询，基于SQLsmith构建
		- Library-based Injection
			- 为增强通用型，fawkes利用library-based injection策略，拦截文件系统级和内核级调用函数，嵌入自定义故障逻辑以按要求使DBMS crash
		- Bug Reproduction
			- 为复现bug，一旦检测到DDB，fawkes就收集最近的checkpoint的workload（数据、查询）、site、故障类型与观察到的行为
	5. Evaluation
		- RQ1:能在现实DBMS中找到DDB吗
		- RQ2:与最新技术相比，Fawkes性能如何
		- RQ3:fawkes每个组件有效性如何
		- RQ4:fawkes能重新找到study中收集的多少bug
		- 测试选择了8个广泛使用的DBMS及其他商业DBMS
		- RQ1:将fawkes应用于8个DBMS中，检测2周的持久性问题，发现了48个独特 未知DDB
		- RQ2:将fawkes与现有的最先进的故障注入工具比较，在各个DBMS运行各个工具72h，记录检测的DDB以及覆盖分支数作为指标。两个指标都最优，前者源于全面的故障注入和选择策略。
		- RQ3:消融实验，各个组件都有提升
		- RQ4:第一周重新发现了43个bug中的34个，两周结束时，发现了43个中的39个
    - **Comment**:
	    - 这篇文章根据对bug的study提出了新的技术。他的着眼点主要是数据持久性问题上，之前没看到过。
- **Title**:DDLumos: Understanding and Detecting Atomic DDL Bugs in DBMSs
    - **Author**:Zhiyong Wu
    - **Contexts**:
	1. Introduction
		- Atomic Data Definition Language（Atomic DDL）
		- 保证schema在修改时完全执行或完全不执行
		- 传统DDL在无回滚机制下直接修改schema。任何中断与故障可能导致DBMS不一致
		- 定义Atomic DDL bug（ADB）为在执行Atomic DDL操作期间出现的问题
		- ADB特征：widespread impact，high severity，complex and hard to detect
		- ADB目前缺乏测试方式，当前方法为模糊测试、随机查询生成，主要针对功能正确性、性能错误、内存安全，侧重于生成复杂度SQL语法结构或SQL操作序列
	2. General Findings of ADBs
		- 分析了收集到的unique的ADB
		- Finding1:44%的bug表现为结果错误，32%的bug表现为恢复后数据不一致，24%的bug表现为系统不可用
		- Finding2：ADB主要的四个原因：32%为不完整的回滚机制，22%为不正确的同步（schema修改导致元数据更新但由于原子操作中断导致数据结构没变），18%为未充分实施并发控制，22%错误处理不正确
		- Finding3:93%ADB是在涉及DDL语句间数据冲突的场景中触发的，其中多个DDL语句在测试用例中操作或与相同元数据元素交互
			- metadata conflict point是触发ADB的关键因素
		- Finding4：Incorrect result，post-recovery data inconsistency，system unavailablity有不同的触发场景与测试oracle
		- 当前技术问题：大多现有工具专注于生成DQL，对DDL的关注少，且生成很少的metadata conflict point；很难模拟不同DDL间的复杂交互；且当前工具很难检测ADB的表现，包括数据不一致、恢复后不一致等
	3. Design of DDLumos
		- 因此提出了DDLumos，由两部分组成了，Test case generator 与 metadta consistency analysis
		1. 初始化元数据表，记录测试用例生成期间用的schema元素及每个DDL操作中的metadata conflict points
		2. 交替生成DDL语句及其他SQL语句，每次DDL操作后，分析生成的元数据冲突点并相应更新元数据表，指导后续DDL生成以最大化冲突场景
		3. 为生成的测试用例构建元数据图并将其发送至目标DBMS，元数据图捕捉DDL前后schema元素间关系，分析schema一致性与正确性
		4. 用元数据图将执行结果与预定义的三个bug表现进行比较从而检测ABD
		- metadata conflict-guided DDL synthesis
			- 生成测试用例，最大化metadata conflict point，包含两部分：
			- Conflict point tracking
				- DDLumos维护一个元数据表，记录与冲突场景相关的schema元素的信息，如DDL标识符（操作类型）、表名、索引、约束等，每次DDL执行后，更新元数据表中对应条目
			- Test case synthesis
				- 通过trach metadata table中conflict point，将DDL、DML、DQL语句交织在一起。生成测试用例语句，先确定语句类型，再构建骨架，用元数据表把对象填入骨架
				- 先初始化空测试用例T，然后迭代合成语句知道达到所需长度L。
				- 每次迭代，先从元数据表MT中检索当前元数据状态Me与conflict point information（Cp），然后确定生成的语句类型（Stype），具体而言，就是根据预设比例决定是否生成DDL。
				- 若为DDL，则选最不常用的语句类型，否则从非DDL中随机选择。
				- 若生成DDL，按conflict point频率对现有的metadata进行排序，用储存的元数据Mu与对应的conflict point生成DDL；若生成DML、DQL，用现有元数据Me合成
				- 一旦语句S合成，更新MT并将S加入T中
		- Graph-based consistency analysis
			- 生成测试用例后，将测试用例分不到多个客户端线程中并控制并发
			- 采用Graph-based consistency analysis来验证执行结果正确性。构造元数据图，基于DDL分析执行后预期的元数据，再与执行后实际元数据比较
			- Metadata graph construction
				- 侧重于基本元数据信息，包括表、列、约束、trigger、行、索引、视图及其关系
				- 每个元素都被建模为图中的一个节点
			- Metadata consistency analysis
				- 对每个DDL语句，识别受影响的元数据对象，检查图与实际是否一致
			- 给定一个测试用例，DDLumos先将其拆为单独的SQL语句，对每一个语句构建并更新元数据图以表示执行后的元数据状态
			- 然后执行SQL语句，若系统执行中未响应，则认为是系统不可用问题
			- 执行后比较实际元数据状态（M）与预期元数据状态（G），二者任何差异都会被认为结果不正确
			- DDLumos还会发送中止信号异步模拟服务器崩溃，重启后检索恢复后的元数据并将其与元数据图比较，任何不匹配会被标记为数据不一致。
	4. Evaluation
		- RQ1:DDLumos对发现现实DBMS中ADB是否有效
		- RQ2:DDLumos与现有技术相比效果如何
		- RQ3:本文study收集的bug，DDLumos能重新找到多少
		- 选择mysql、mariaDB、percona、polarDB、greatSQL、PostgreSQL用于测试
		- RQ1:DDLumos在2周内找到72个ADB
		- RQ2:评估三个指标：DDL操作中覆盖的代码分支量；48h内，每个DDL语句评价生成的metadata conflict point量；48h内，各工具发现unique ADB数量
			- 三个指标都是DDLumos最优
		- RQ3:前2天，DDLumos重新发现了207个ADB中的179个，一周共找到207个中的196个
    - **Comment**:
	    - 这篇主要是处理ADB问题，并提出了DDLumos