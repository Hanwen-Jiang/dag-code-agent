// XAI Dashboard Frontend Application
class XAIDashboard {
    constructor() {
        this.selectedTaskId = null;
        this.ws = null;
        this.init();
    }

    async init() {
        await this.connectWebSocket();
        await this.loadTraces();
        this.setupEventListeners();
    }

    async connectWebSocket() {
        this.ws = new EventSource('/ws/traces');
        
        this.ws.onmessage = (event) => {
            const data = JSON.parse(event.data);
            this.handleTraceEvent(data);
        };

        this.ws.onerror = (error) => {
            console.error('WebSocket error:', error);
            document.getElementById('connectionStatus').textContent = 'Disconnected';
        };
    }

    async loadTraces() {
        try {
            const response = await fetch('/api/traces');
            const data = await response.json();
            
            this.renderTaskList(data.traces);
        } catch (error) {
            console.error('Failed to load traces:', error);
        }
    }

    renderTaskList(traces) {
        const container = document.getElementById('taskList');
        container.innerHTML = '';
        
        if (!traces || traces.length === 0) {
            container.innerHTML = '<div class="no-tasks">No tasks found</div>';
            return;
        }
        
        traces.forEach(trace => {
            const taskCard = this.createTaskCard(trace);
            container.appendChild(taskCard);
        });
    }

    createTaskCard(trace) {
        const div = document.createElement('div');
        div.className = `task-card ${this.selectedTaskId === trace.TaskID ? 'selected' : ''}`;
        if (trace.Status === 'completed') div.classList.add('success');
        else if (trace.Status === 'failed') div.classList.add('failed');
        
        const statusClass = {
            'running': 'status-running',
            'completed': 'status-completed',
            'failed': 'status-failed',
            'timed_out': 'status-timeout'
        }[trace.Status] || '';
        
        div.innerHTML = `
            <div class="task-header">
                <span class="task-id">${trace.TaskID.substring(0, 8)}...</span>
                <span class="status-badge ${statusClass}">${trace.Status}</span>
            </div>
            <div class="task-meta">
                <div>Model: ${trace.ModelUsed}</div>
                <div>Tokens: ${trace.TotalTokens.toLocaleString()}</div>
                <div>Cost: $${trace.Cost.toFixed(2)}</div>
                <div>Duration: ${this.formatDuration(trace.CreatedAt)}</div>
            </div>
            <button class="view-btn" onclick="selectTask('${trace.TaskID}')">View Details</button>
        `;
        
        div.onclick = (e) => {
            if (!e.target.classList.contains('view-btn')) {
                selectTask(trace.TaskID);
            }
        };
        
        return div;
    }

    async selectTask(taskId) {
        this.selectedTaskId = taskId;
        
        // Update UI selection
        document.querySelectorAll('.task-card').forEach(card => {
            card.classList.remove('selected');
        });
        
        // Load trace details
        await this.loadTraceDetails(taskId);
        await this.loadAttributionAnalysis(taskId);
        await this.loadExplanation(taskId);
        await this.updateMetrics(taskId);
        
        // Re-render task list with updated selection
        await this.loadTraces();
    }

    async loadTraceDetails(taskId) {
        try {
            const response = await fetch(`/api/trace?task_id=${taskId}`);
            const trace = await response.json();
            
            this.renderTraceVisualization(trace);
            document.getElementById('selectedTaskInfo').innerHTML = `
                <div class="task-info">
                    <strong>${taskId}</strong> • ${trace.Status}
                </div>
            `;
        } catch (error) {
            console.error('Failed to load trace:', error);
        }
    }

    renderTraceVisualization(trace) {
        const cy = cytoscape({
            container: document.getElementById('traceVisualization'),
            
            elements: {
                nodes: [
                    { data: { id: 'start', label: 'Start', group: 'control' } },
                    ...trace.Steps.map((step, i) => ({
                        data: { 
                            id: `step_${i}`,
                            label: `Step ${step.Index + 1}`,
                            type: this.getStepType(step),
                            toolCount: step.ToolCalls?.length || 0,
                            latency: step.LatencyMs.toFixed(0)
                        }
                    })),
                    { data: { id: 'end', label: 'End', group: 'control' } }
                ],
                
                edges: [
                    { data: { id: 'start-to-first', source: 'start', target: 'step_0' } },
                    ...trace.Steps.map((step, i) => {
                        if (i < trace.Steps.length - 1) {
                            return {
                                data: {
                                    id: `edge-${i}`,
                                    source: `step_${i}`,
                                    target: `step_${i + 1}`
                                }
                            };
                        }
                        return {
                            data: {
                                id: 'last-to-end',
                                source: `step_${trace.Steps.length - 1}`,
                                target: 'end'
                            }
                        };
                    })
                ]
            },
            
            style: [
                {
                    selector: 'node',
                    style: {
                        'label': 'data(label)',
                        'shape': 'round-rectangle',
                        'background-color': '#666',
                        'color': '#fff',
                        'text-valign': 'center',
                        'text-halign': 'center',
                        'width': 100,
                        'height': 50
                    }
                },
                {
                    selector: "node[group='control']",
                    style: {
                        'background-color': '#3498db',
                        'shape': 'diamond'
                    }
                },
                {
                    selector: ":hover",
                    style: {
                        'border-width': 4,
                        'border-color': '#f39c12',
                        'line-color': '#f39c12'
                    }
                },
                {
                    selector: 'edge',
                    style: {
                        'width': 2,
                        'line-color': '#ccc',
                        'target-arrow-color': '#ccc',
                        'target-arrow-shape': 'triangle',
                        'curve-style': 'bezier'
                    }
                }
            ],
            
            layout: {
                name: 'preset'
            }
        });
    }

    getStepType(step) {
        if (step.LoopDecision?.triggered) return 'loop-detection';
        if (step.PermissionCheck) return 'permission-check';
        if (step.ToolCalls && step.ToolCalls.length > 0) return 'tool-execution';
        if (step.StateAfterLLM?.memory_query) return 'memory-query';
        return 'llm-reasoning';
    }

    async loadAttributionAnalysis(taskId) {
        try {
            const response = await fetch(`/api/attribution?task_id=${taskId}`);
            const report = await response.json();
            
            this.renderAttributionReport(report);
        } catch (error) {
            console.error('Failed to load attribution:', error);
        }
    }

    renderAttributionReport(report) {
        const container = document.getElementById('attributionContent');
        
        let html = '<div class="attribution-report">';
        
        // Key Steps
        if (report.key_steps && report.key_steps.length > 0) {
            html += '<h4>Key Contributing Steps</h4><ul>';
            report.key_steps.forEach(step => {
                const impactClass = step.contribution > 0.1 ? 'impact-positive' : 
                                   step.contribution < -0.1 ? 'impact-negative' : '';
                html += `
                    <li class="${impactClass}">
                        Step ${step.index}: ${step.description}
                        <br/>Impact: ${step.contribution.toFixed(2)} | Latency: ${step.latency_ms.toFixed(0)}ms
                    </li>
                `;
            });
            html += '</ul>';
        } else {
            html += '<p>No significant steps detected.</p>';
        }
        
        // Critical Decisions
        if (report.critical_decisions && report.critical_decisions.length > 0) {
            html += '<h4>Critical Decisions</h4><ul>';
            report.critical_decisions.forEach(decision => {
                html += `
                    <li>
                        <strong>${decision.decision_type}</strong>: ${decision.rationale}
                        <br/>Confidence: ${(decision.confidence * 100).toFixed(0)}%
                    </li>
                `;
            });
            html += '</ul>';
        }
        
        // Root Causes (if failed)
        if (report.failure_root_causes && report.failure_root_causes.length > 0) {
            html += '<h4>Failure Root Causes</h4><ul>';
            report.failure_root_causes.forEach(cause => {
                html += `
                    <li class="failure">
                        <strong>${cause.cause_type}</strong>: ${cause.description}
                        <br/>Suggestion: ${cause.suggestion}
                    </li>
                `;
            });
            html += '</ul>';
        }
        
        html += '</div>';
        container.innerHTML = html;
    }

    async loadExplanation(taskId) {
        try {
            const response = await fetch(`/api/explanation?task_id=${taskId}`);
            const exp = await response.json();
            
            this.renderExplanation(exp);
        } catch (error) {
            console.error('Failed to load explanation:', error);
        }
    }

    renderExplanation(exp) {
        const container = document.getElementById('explanationContent');
        
        let html = `<div class="explanation-container">
            <div class="certainty-indicator">
                Certainty: ${Math.round(exp.certainty * 100)}%
            </div>`;
        
        // Reasoning path
        if (exp.reasoning_path && exp.reasoning_path.length > 0) {
            html += '<h4>Execution Flow</h4><ol>';
            exp.reasoning_path.slice(0, 5).forEach(seg => {
                html += `
                    <li>
                        <strong>Intent:</strong> ${seg.intent}
                        <br/>
                        <strong>Action:</strong> ${seg.action}
                        <br/>
                        <strong>Outcome:</strong> ${seg.outcome}
                    </li>
                `;
            });
            if (exp.reasoning_path.length > 5) {
                html += `<li>...and ${exp.reasoning_path.length - 5} more steps</li>`;
            }
            html += '</ol>';
        }
        
        // Key decisions
        if (exp.key_decisions && exp.key_decisions.length > 0) {
            html += '<h4>Key Decisions</h4><ul>';
            exp.key_decisions.forEach(dec => {
                html += `
                    <li>
                        <strong>${dec.decision_type}</strong>
                        <br/>Rationale: ${dec.rationale}
                        ${dec.uncertainty ? `<br/><em>Uncertainty: ${dec.uncertainty}</em>` : ''}
                    </li>
                `;
            });
            html += '</ul>';
        }
        
        html += '</div>';
        container.innerHTML = html;
    }

    async updateMetrics(taskId) {
        try {
            const traceResponse = await fetch(`/api/trace?task_id=${taskId}`);
            const trace = await traceResponse.json();
            
            const analysisResponse = await fetch(`/api/attribution?task_id=${taskId}`);
            const attribution = await analysisResponse.json();
            
            const metricsData = {
                successRate: trace.status === 'completed' ? 100 : 0,
                p95Latency: attribution.p95_latency_ms || 0,
                avgCost: trace.cost || 0,
                tokenEfficiency: attribution.avg_tokens_per_task || 0,
                loopEvents: attribution.loop_events || 0,
                permissionDenials: attribution.permission_blocked || 0,
                memoryQueries: attribution.memory_queries || 0
            };
            
            this.renderMetrics(metricsData);
        } catch (error) {
            console.error('Failed to update metrics:', error);
        }
    }

    renderMetrics(metrics) {
        const grid = document.getElementById('metricsGrid');
        
        const metricCards = [
            { label: 'Success Rate', value: `${metrics.successRate}%`, unit: '%', color: '#4CAF50' },
            { label: 'P95 Latency', value: (metrics.p95Latency / 1000).toFixed(1), unit: 's', color: '#2196F3' },
            { label: 'Avg Cost', value: `$${metrics.avgCost.toFixed(3)}`, unit: '/task', color: '#FF9800' },
            { label: 'Loop Events', value: metrics.loopEvents, unit: 'times', color: '#9C27B0' },
            { label: 'Permission Denials', value: metrics.permissionDenials, unit: 'times', color: '#F44336' },
            { label: 'Memory Queries', value: metrics.memoryQueries, unit: 'times', color: '#00BCD4' },
        ];
        
        grid.innerHTML = metricCards.map(m => `
            <div class="metric-card">
                <div class="metric-value" style="color: ${m.color}">${m.value}</div>
                <div class="metric-label">${m.label}</div>
                <div class="metric-unit">${m.unit}</div>
            </div>
        `).join('');
    }

    handleTraceEvent(data) {
        console.log('Received trace event:', data);
        
        if (data.type === 'trace_started') {
            // Auto-refresh to show new task
            this.loadTraces();
        }
    }

    setupEventListeners() {
        document.getElementById('refreshBtn').onclick = () => this.loadTraces();
    }

    formatDuration(timestamp) {
        const date = new Date(timestamp);
        const now = new Date();
        const diff = Math.floor((now - date) / 1000);
        
        if (diff < 60) return `${diff}s ago`;
        if (diff < 3600) return `${Math.floor(diff / 60)}m ago`;
        return `${Math.floor(diff / 3600)}h ago`;
    }

    closeModal() {
        document.getElementById('detailModal').style.display = 'none';
    }
}

// Initialize dashboard
const dashboard = new XAIDashboard();

// Global functions for HTML onclick handlers
window.selectTask = (taskId) => dashboard.selectTask(taskId);
window.showTab = (tabName) => {
    document.querySelectorAll('.tab-content').forEach(content => {
        content.classList.remove('active');
    });
    document.querySelectorAll('.tab').forEach(tab => {
        tab.classList.remove('active');
    });
    document.getElementById(`${tabName}Panel`).classList.add('active');
    event.target.classList.add('active');
};

