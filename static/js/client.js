/**
 * StroyControl — скрипты для дашборда клиента
 * Lightbox: открытие фото на весь экран по клику
 */

(function () {
    'use strict';

    var lightbox = document.getElementById('lightbox');
    var lightboxImg = document.querySelector('.lightbox-img');
    var lightboxClose = document.querySelector('.lightbox-close');
    var photoLinks = document.querySelectorAll('[data-fullscreen]');

    // Открыть фото в lightbox
    photoLinks.forEach(function (link) {
        link.addEventListener('click', function (e) {
            e.preventDefault();
            var imgSrc = link.getAttribute('href');
            if (imgSrc && lightboxImg) {
                lightboxImg.src = imgSrc;
                lightbox.classList.add('active');
            }
        });
    });

    // Закрыть lightbox
    function closeLightbox() {
        lightbox.classList.remove('active');
    }

    if (lightboxClose) {
        lightboxClose.addEventListener('click', closeLightbox);
    }

    lightbox.addEventListener('click', function (e) {
        if (e.target === lightbox) {
            closeLightbox();
        }
    });

    // Закрытие по Escape
    document.addEventListener('keydown', function (e) {
        if (e.key === 'Escape' && lightbox.classList.contains('active')) {
            closeLightbox();
        }
    });
})();
