/**
 * StroyControl — скрипты для дашборда мастера
 * Модальные окна: добавление отчёта (с подтверждением закрытия этапа), редактирование
 */

(function () {
    'use strict';

    var reportModal = document.getElementById('reportModal');
    var editModal = document.getElementById('editModal');
    var addReportBtns = document.querySelectorAll('.add-report-btn');
    var editStageBtns = document.querySelectorAll('.edit-stage-btn');
    var closeBtns = document.querySelectorAll('.modal-close');
    var editModalClose = document.querySelector('.edit-modal-close');
    var modalStageName = document.getElementById('modalStageName');
    var modalStageId = document.getElementById('modalStageId');
    var reportForm = document.getElementById('reportForm');
    var editForm = document.getElementById('editForm');
    var photoInput = document.getElementById('photo');
    var photoEditInput = document.getElementById('photo_edit');

    // Открыть модалку добавления отчёта
    addReportBtns.forEach(function (btn) {
        btn.addEventListener('click', function () {
            var stageId = btn.getAttribute('data-stage-id');
            var stageName = btn.getAttribute('data-stage-name');
            modalStageId.value = stageId;
            modalStageName.textContent = stageName;
            reportForm.dataset.hasReports = btn.getAttribute('data-has-reports') || '0';
            photoInput.value = '';
            if (reportForm.querySelector('#comment')) {
                reportForm.querySelector('#comment').value = '';
            }
            reportModal.classList.add('active');
        });
    });

    // Открыть модалку редактирования (с подтверждением)
    editStageBtns.forEach(function (btn) {
        btn.addEventListener('click', function () {
            var stageId = btn.getAttribute('data-stage-id');
            var stageName = btn.getAttribute('data-stage-name');
            var confirmed = confirm('Вы действительно хотите отредактировать этап? Заявка будет отправлена на рассмотрение администратору.');
            if (confirmed) {
                document.getElementById('editModalStageId').value = stageId;
                document.getElementById('editModalStageName').textContent = stageName;
                photoEditInput.value = '';
                if (editForm.querySelector('#comment_edit')) {
                    editForm.querySelector('#comment_edit').value = '';
                }
                editModal.classList.add('active');
            }
        });
    });

    // Закрытие модалок
    function closeReportModal() {
        reportModal.classList.remove('active');
    }
    function closeEditModal() {
        editModal.classList.remove('active');
    }

    closeBtns.forEach(function (btn) {
        btn.addEventListener('click', function () {
            if (btn.classList.contains('edit-modal-close')) {
                closeEditModal();
            } else {
                closeReportModal();
            }
        });
    });

    reportModal.addEventListener('click', function (e) {
        if (e.target === reportModal) closeReportModal();
    });
    editModal.addEventListener('click', function (e) {
        if (e.target === editModal) closeEditModal();
    });

    // Подтверждение при закрытии этапа (первый отчёт)
    if (reportForm && photoInput) {
        reportForm.addEventListener('submit', function (e) {
            if (!photoInput.files || photoInput.files.length === 0) {
                e.preventDefault();
                alert('Выберите хотя бы одну фотографию.');
                return;
            }
            var hasReports = reportForm.dataset.hasReports === '1';
            if (!hasReports) {
                var confirmClose = confirm('Вы действительно закрываете этап и подтверждаете его выполнение?');
                if (!confirmClose) {
                    e.preventDefault();
                }
            }
        });
    }

    // Проверка фото при отправке заявки на редактирование
    if (editForm && photoEditInput) {
        editForm.addEventListener('submit', function (e) {
            if (!photoEditInput.files || photoEditInput.files.length === 0) {
                e.preventDefault();
                alert('Выберите хотя бы одну новую фотографию.');
            }
        });
    }

    // Lightbox для просмотра фото на весь экран (как у клиента)
    var lightbox = document.getElementById('lightbox');
    var lightboxImg = document.querySelector('.lightbox-img');
    var lightboxClose = document.querySelector('#lightbox .lightbox-close');
    var photoLinks = document.querySelectorAll('.photo-open-full, [data-fullscreen]');

    photoLinks.forEach(function (link) {
        link.addEventListener('click', function (e) {
            e.preventDefault();
            var imgSrc = link.getAttribute('href');
            if (imgSrc && lightboxImg && lightbox) {
                lightboxImg.src = imgSrc;
                lightbox.classList.add('active');
            }
        });
    });

    if (lightboxClose) {
        lightboxClose.addEventListener('click', function () {
            lightbox.classList.remove('active');
        });
    }
    if (lightbox) {
        lightbox.addEventListener('click', function (e) {
            if (e.target === lightbox) lightbox.classList.remove('active');
        });
    }
    document.addEventListener('keydown', function (e) {
        if (e.key === 'Escape' && lightbox && lightbox.classList.contains('active')) {
            lightbox.classList.remove('active');
        }
    });
})();
