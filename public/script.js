class KafkaUI {
    constructor() {
        this.ws = null;
        this.messages = [];
        this.filteredMessages = [];
        this.currentTopic = null;
        this.searchTerm = '';
        this.isSubscribing = false;
        
        this.initElements();
        this.initWebSocket();
        this.loadTopics();
        this.bindEvents();
    }

    initElements() {
        this.topicSelect = document.getElementById('topicSelect');
        this.subscribeBtn = document.getElementById('subscribeBtn');
        this.messagesContainer = document.getElementById('messages');
        this.messageCount = document.getElementById('messageCount');
        
        // Search elements
        this.globalSearch = document.getElementById('globalSearch');
        this.clearSearchBtn = document.getElementById('clearSearchBtn');
        
        // Status elements
        this.statusIndicator = document.getElementById('statusIndicator');
        this.statusText = document.getElementById('statusText');
    }

    initWebSocket() {
        const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
        const wsUrl = `${protocol}//${window.location.host}`;
        
        this.ws = new WebSocket(wsUrl);

        // Set initial connecting state
        this.updateConnectionStatus('connecting', 'Connecting...');

        this.ws.onopen = () => {
            console.log('WebSocket connected');
            this.updateConnectionStatus('connected', 'Kafka connected');
            // Ensure we show the correct initial state
            this.renderMessages();
        };

        this.ws.onmessage = (event) => {
            const data = JSON.parse(event.data);
            this.handleWebSocketMessage(data);
        };

        this.ws.onclose = () => {
            console.log('WebSocket disconnected');
            this.updateConnectionStatus('disconnected', 'Disconnected');
            setTimeout(() => this.initWebSocket(), 3000);
        };

        this.ws.onerror = (error) => {
            console.error('WebSocket error:', error);
            this.updateConnectionStatus('disconnected', 'Error');
        };
    }

    handleWebSocketMessage(data) {
        switch (data.type) {
            case 'messages':
                this.messages = data.data;
                console.log('Received messages:', this.messages.length);
                this.applySearch();
                break;
            case 'newMessage':
                this.messages.unshift(data.data);
                if (this.messages.length > 100) {
                    this.messages = this.messages.slice(0, 100);
                }
                this.applySearch();
                break;
            case 'status':
                this.isSubscribing = false;
                this.renderMessages();
                break;
            case 'error':
                this.isSubscribing = false;
                this.renderMessages();
                break;
        }
    }

    async loadTopics() {
        try {
            const response = await fetch('/api/topics');
            const topics = await response.json();
            
            this.topicSelect.innerHTML = '<option value="">Select topic...</option>';
            topics.forEach(topic => {
                const option = document.createElement('option');
                option.value = topic;
                option.textContent = topic;
                this.topicSelect.appendChild(option);
            });
        } catch (error) {
            console.error('Error loading topics:', error);
            alert('Error loading topics');
        }
    }

    bindEvents() {
        this.subscribeBtn.addEventListener('click', () => {
            const topic = this.topicSelect.value;
            if (!topic) {
                alert('Please select a topic');
                return;
            }
            this.subscribeTopic(topic);
        });

        // Enter key auf Topic Select
        this.topicSelect.addEventListener('keypress', (e) => {
            if (e.key === 'Enter') {
                this.subscribeBtn.click();
            }
        });

        // Search event listeners
        this.globalSearch.addEventListener('input', () => {
            this.searchTerm = this.globalSearch.value.toLowerCase();
            console.log('Search term changed to:', this.searchTerm);
            this.applySearch();
        });

        this.clearSearchBtn.addEventListener('click', () => {
            this.clearSearch();
        });

        // Enter key on search
        this.globalSearch.addEventListener('keypress', (e) => {
            if (e.key === 'Enter') {
                // Focus stays on search, just apply search again
                this.applySearch();
            }
        });
    }

    subscribeTopic(topic) {
        if (this.ws && this.ws.readyState === WebSocket.OPEN) {
            this.isSubscribing = true;
            this.currentTopic = topic; // Set topic before rendering
            this.messages = []; // Clear previous messages
            this.renderMessages(); // Show loading spinner
            
            this.ws.send(JSON.stringify({
                type: 'subscribe',
                topic: topic
            }));
            console.log(`Subscribing to topic: ${topic}`);
        } else {
            alert('WebSocket not connected');
        }
    }

    updateConnectionStatus(state, text) {
        console.log(`Connection status: ${state} - ${text}`);
        // Remove all status classes
        this.statusIndicator.className = 'status-indicator';
        // Add new status class
        this.statusIndicator.classList.add(state);
        // Update text
        this.statusText.textContent = text;
    }

    applySearch() {
        console.log('Applying search:', this.searchTerm);
        console.log('Total messages before search:', this.messages.length);
        
        if (!this.searchTerm) {
            // No search term, show all messages
            this.filteredMessages = this.messages;
        } else {
            this.filteredMessages = this.messages.filter(msg => {
                return this.searchInMessage(msg, this.searchTerm);
            });
        }

        console.log('Filtered messages count:', this.filteredMessages.length);
        this.renderMessages();
    }

    searchInMessage(msg, searchTerm) {
        // Convert search term to lowercase for case-insensitive search
        const term = searchTerm.toLowerCase();
        
        // Search in message key
        if (msg.key && msg.key.toLowerCase().includes(term)) {
            return true;
        }
        
        // Search in message value
        if (msg.value && msg.value.toLowerCase().includes(term)) {
            return true;
        }
        
        // Search in topic
        if (msg.topic && msg.topic.toLowerCase().includes(term)) {
            return true;
        }
        
        // Search in headers
        if (msg.headers) {
            for (const [headerKey, headerValue] of Object.entries(msg.headers)) {
                if (headerKey.toLowerCase().includes(term)) {
                    return true;
                }
                if (headerValue && headerValue.toString().toLowerCase().includes(term)) {
                    return true;
                }
            }
        }
        
        // Try to parse value as JSON and search in nested fields
        if (msg.value) {
            try {
                const parsed = JSON.parse(msg.value);
                if (this.searchInObject(parsed, term)) {
                    return true;
                }
            } catch (e) {
                // Not JSON, already searched as string above
            }
        }
        
        return false;
    }

    searchInObject(obj, searchTerm) {
        if (typeof obj === 'string') {
            return obj.toLowerCase().includes(searchTerm);
        }
        
        if (typeof obj === 'number') {
            return obj.toString().toLowerCase().includes(searchTerm);
        }
        
        if (Array.isArray(obj)) {
            return obj.some(item => this.searchInObject(item, searchTerm));
        }
        
        if (obj && typeof obj === 'object') {
            for (const [key, value] of Object.entries(obj)) {
                // Search in key names
                if (key.toLowerCase().includes(searchTerm)) {
                    return true;
                }
                // Search in values recursively
                if (this.searchInObject(value, searchTerm)) {
                    return true;
                }
            }
        }
        
        return false;
    }

    clearSearch() {
        this.searchTerm = '';
        this.globalSearch.value = '';
        this.applySearch();
    }

    renderMessages() {
        const totalMessages = this.messages.length;
        const filteredCount = this.filteredMessages.length;
        
        // Update message counter to show search results vs total
        if (this.searchTerm && totalMessages !== filteredCount) {
            this.messageCount.textContent = `${filteredCount} von ${totalMessages}`;
        } else {
            this.messageCount.textContent = totalMessages;
        }

        if (this.isSubscribing) {
            this.messagesContainer.innerHTML = `
                <div class="loading-container">
                    <div class="spinner"></div>
                    <div class="loading-text">Connecting to topic "${this.currentTopic}"...</div>
                </div>
            `;
            return;
        }

        if (this.filteredMessages.length === 0) {
            if (totalMessages === 0 && !this.currentTopic) {
                this.messagesContainer.innerHTML = `
                    <div class="no-messages">
                        No messages available. Select a topic and click Subscribe.
                    </div>
                `;
            } else if (totalMessages === 0 && this.currentTopic) {
                this.messagesContainer.innerHTML = `
                    <div class="no-messages">
                        No messages found in topic "${this.currentTopic}".
                    </div>
                `;
            } else {
                this.messagesContainer.innerHTML = `
                    <div class="no-messages">
                        No messages found for "${this.searchTerm}". 
                        <br><small>${totalMessages} messages available in total.</small>
                    </div>
                `;
            }
            return;
        }

        const messagesHtml = this.filteredMessages.map(msg => this.renderMessage(msg)).join('');
        this.messagesContainer.innerHTML = messagesHtml;
    }

    renderMessage(msg) {
        let timestampDisplay;
        if (msg.timestamp) {
            timestampDisplay = new Date(msg.timestamp).toLocaleString('de-DE', {
                day: '2-digit',
                month: '2-digit', 
                year: 'numeric',
                hour: '2-digit',
                minute: '2-digit',
                second: '2-digit'
            });
        } else {
            // Fallback to receivedAt timestamp with indicator
            timestampDisplay = new Date(msg.receivedAt).toLocaleString('de-DE', {
                day: '2-digit',
                month: '2-digit', 
                year: 'numeric',
                hour: '2-digit',
                minute: '2-digit',
                second: '2-digit'
            }) + ' (received)';
        }
        
        let value = msg.value || '';
        
        try {
            // Try to format JSON for better readability
            const parsed = JSON.parse(value);
            value = JSON.stringify(parsed, null, 2);
        } catch (e) {
            // Not JSON, show as plain text
        }

        return `
            <div class="message">
                <div class="message-header">
                    <span class="message-topic">${this.escapeHtml(msg.topic)}</span>
                    <div class="message-meta">
                        <span>⏰ ${timestampDisplay}</span>
                    </div>
                </div>
                <div class="message-content">
                    <div class="message-value">${this.escapeHtml(value)}</div>
                </div>
            </div>
        `;
    }

    escapeHtml(text) {
        const div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    }
}

// Initialize the app when DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
    new KafkaUI();
});