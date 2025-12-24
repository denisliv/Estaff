// Глобальные переменные
let ws = null;
let resumeModal = null;

// Инициализация при загрузке страницы
document.addEventListener('DOMContentLoaded', function() {
    resumeModal = new bootstrap.Modal(document.getElementById('resumeModal'));
    initWebSocket();
    initEventListeners();
    updateCollectionStatus();
    // Обновляем статус коллекции каждые 30 секунд
    setInterval(updateCollectionStatus, 30000);
});

// Инициализация WebSocket для логов
function initWebSocket() {
    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const wsUrl = `${protocol}//${window.location.host}/api/v1/ws/logs`;
    
    ws = new WebSocket(wsUrl);
    
    ws.onopen = function() {
        addLog('Система', 'Подключение к серверу установлено', 'info');
    };
    
    ws.onmessage = function(event) {
        const logData = JSON.parse(event.data);
        addLog(
            logData.module || 'Система', 
            logData.message, 
            logData.level.toLowerCase(),
            logData.timestamp
        );
    };
    
    ws.onerror = function(error) {
        console.error('WebSocket error:', error);
        addLog('Система', 'Ошибка подключения к серверу', 'error');
    };
    
    ws.onclose = function() {
        addLog('Система', 'Соединение с сервером закрыто. Переподключение...', 'warning');
        // Переподключение через 3 секунды
        setTimeout(initWebSocket, 3000);
    };
}

// Инициализация обработчиков событий
function initEventListeners() {
    const searchForm = document.getElementById('searchForm');
    const clearLogsBtn = document.getElementById('clearLogsBtn');
    
    searchForm.addEventListener('submit', handleSearch);
    clearLogsBtn.addEventListener('click', clearLogs);
}

// Обработка поиска кандидатов
async function handleSearch(event) {
    event.preventDefault();
    
    const searchBtn = document.getElementById('searchBtn');
    const searchSpinner = document.getElementById('searchSpinner');
    const resultsContainer = document.getElementById('resultsContainer');
    
    // Получаем данные формы
    const description = document.getElementById('vacancyDescription').value.trim();
    const experienceMin = document.getElementById('experienceMin').value;
    const grade = document.getElementById('grade').value;
    const k = parseInt(document.getElementById('k').value) || 5;
    
    if (!description) {
        alert('Пожалуйста, введите описание вакансии');
        return;
    }
    
    // Показываем индикатор загрузки
    searchBtn.disabled = true;
    searchSpinner.classList.remove('d-none');
    resultsContainer.innerHTML = '<div class="text-center py-5"><div class="spinner-border text-primary" role="status"></div><p class="mt-3">Поиск кандидатов...</p></div>';
    
    try {
        // Формируем запрос
        const requestBody = {
            description: description,
            k: k
        };
        
        if (experienceMin) {
            requestBody.experience_years_min = parseFloat(experienceMin);
        }
        
        if (grade) {
            requestBody.grade = grade;
        }
        
        // Отправляем запрос
        const response = await fetch('/api/v1/search', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(requestBody)
        });
        
        if (!response.ok) {
            const errorData = await response.json();
            throw new Error(errorData.detail || 'Ошибка при поиске кандидатов');
        }
        
        const data = await response.json();
        
        // Отображаем результаты
        displayResults(data.candidates || []);
        
    } catch (error) {
        console.error('Ошибка поиска:', error);
        resultsContainer.innerHTML = `
            <div class="alert alert-danger" role="alert">
                <strong>Ошибка:</strong> ${error.message}
            </div>
        `;
    } finally {
        // Скрываем индикатор загрузки
        searchBtn.disabled = false;
        searchSpinner.classList.add('d-none');
    }
}

// Отображение результатов поиска
function displayResults(candidates) {
    const resultsContainer = document.getElementById('resultsContainer');
    
    if (candidates.length === 0) {
        resultsContainer.innerHTML = `
            <div class="empty-state">
                <div class="empty-state-icon">🔍</div>
                <h5>Кандидаты не найдены</h5>
                <p>Попробуйте изменить параметры поиска</p>
            </div>
        `;
        return;
    }
    
    let html = `<div class="mb-3"><strong>Найдено кандидатов: ${candidates.length}</strong></div>`;
    
    candidates.forEach((candidate, index) => {
        const relevanceScore = candidate.relevance_score || 0;
        const hardSkillsScore = candidate.hard_skills_score || 0;
        const domainSkillsScore = candidate.domain_skills_score || 0;
        
        html += `
            <div class="candidate-card">
                <div class="candidate-header">
                    <div>
                        <div class="candidate-name">${escapeHtml(candidate.name)}</div>
                        <div class="candidate-info">
                            📞 ${escapeHtml(candidate.phone)} | 📍 ${escapeHtml(candidate.location)}
                        </div>
                    </div>
                    <div>
                        <span class="score-badge ${getScoreClass(relevanceScore)}">
                            Релевантность: ${relevanceScore}/10
                        </span>
                    </div>
                </div>
                
                <div class="scores-container">
                    <div class="score-item">
                        <span class="score-label">Хард-скиллы:</span>
                        <span class="score-value ${getScoreClass(hardSkillsScore)}">${hardSkillsScore}/10</span>
                    </div>
                    <div class="score-item">
                        <span class="score-label">Доменные навыки:</span>
                        <span class="score-value ${getScoreClass(domainSkillsScore)}">${domainSkillsScore}/10</span>
                    </div>
                </div>
                
                ${candidate.relevance_explanation ? `
                    <div class="explanation">
                        <strong>Объяснение:</strong> ${escapeHtml(candidate.relevance_explanation)}
                    </div>
                ` : ''}
                
                <div class="mt-3">
                    <button class="btn btn-sm btn-outline-primary" onclick="showResume('${escapeHtml(candidate.name)}', '${escapeHtml(candidate.phone)}')">
                        📄 Показать резюме
                    </button>
                </div>
            </div>
        `;
    });
    
    resultsContainer.innerHTML = html;
}

// Получение класса для оценки
function getScoreClass(score) {
    if (score >= 8) return 'score-excellent';
    if (score >= 6) return 'score-good';
    if (score >= 4) return 'score-average';
    return 'score-poor';
}

// Показать резюме кандидата
async function showResume(name, phone) {
    const resumeContent = document.getElementById('resumeContent');
    resumeContent.innerHTML = '<div class="text-center py-5"><div class="spinner-border text-primary" role="status"></div><p class="mt-3">Загрузка резюме...</p></div>';
    
    resumeModal.show();
    
    try {
        const response = await fetch(`/api/v1/candidate/resume?name=${encodeURIComponent(name)}&phone=${encodeURIComponent(phone)}`);
        
        if (!response.ok) {
            throw new Error('Резюме не найдено');
        }
        
        const html = await response.text();
        resumeContent.innerHTML = html;
        
    } catch (error) {
        console.error('Ошибка загрузки резюме:', error);
        resumeContent.innerHTML = `
            <div class="alert alert-danger" role="alert">
                <strong>Ошибка:</strong> Не удалось загрузить резюме. ${error.message}
            </div>
        `;
    }
}

// Добавление лога в контейнер
function addLog(module, message, level, serverTimestamp = null) {
    const logsContainer = document.getElementById('logsContainer');
    
    // Используем timestamp из сервера, если он есть, иначе текущее время
    const timestamp = serverTimestamp || new Date().toLocaleTimeString('ru-RU');
    
    const logEntry = document.createElement('div');
    logEntry.className = 'log-entry';
    
    const levelClass = `log-level-${level}`;
    
    // Краткое имя модуля для отображения (берем последнюю часть после точки)
    const moduleDisplay = module.split('.').pop() || module;
    
    logEntry.innerHTML = `
        <span class="log-timestamp">[${escapeHtml(timestamp)}]</span>
        <span class="${levelClass}">[${level.toUpperCase()}]</span>
        <span class="log-message">[${escapeHtml(moduleDisplay)}] ${escapeHtml(message)}</span>
    `;
    
    logsContainer.appendChild(logEntry);
    
    // Автопрокрутка вниз
    logsContainer.scrollTop = logsContainer.scrollHeight;
    
    // Ограничение количества логов (последние 200)
    const logs = logsContainer.querySelectorAll('.log-entry');
    if (logs.length > 200) {
        logs[0].remove();
    }
}

// Очистка логов
function clearLogs() {
    document.getElementById('logsContainer').innerHTML = '';
}

// Экранирование HTML
function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

// Обновление статуса коллекции
async function updateCollectionStatus() {
    const statusElement = document.getElementById('collectionStatus');
    
    try {
        const response = await fetch('/api/v1/collection/status');
        
        if (!response.ok) {
            throw new Error('Не удалось получить статус коллекции');
        }
        
        const data = await response.json();
        
        if (data.status === 'ok') {
            const pointsCount = data.points_count || 0;
            statusElement.innerHTML = `
                <span class="badge bg-success">Коллекция: ${pointsCount.toLocaleString('ru-RU')} точек</span>
            `;
        } else {
            throw new Error(data.error || 'Неизвестная ошибка');
        }
    } catch (error) {
        console.error('Ошибка получения статуса коллекции:', error);
        statusElement.innerHTML = `
            <span class="badge bg-danger">Статус недоступен</span>
        `;
    }
}

