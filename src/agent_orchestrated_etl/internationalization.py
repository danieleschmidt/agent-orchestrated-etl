"""Internationalization (I18n) support for Agent-Orchestrated ETL."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

from .logging_config import get_logger


class I18nManager:
    """Manages internationalization and localization for the ETL system."""

    def __init__(self, default_locale: str = "en-US", locale_dir: Optional[str] = None):
        """Initialize I18n manager.
        
        Args:
            default_locale: Default locale code (e.g., 'en-US', 'fr-FR')
            locale_dir: Directory containing translation files
        """
        self.logger = get_logger("agent_etl.i18n")
        self.default_locale = default_locale
        self.current_locale = default_locale

        # Set up locale directory
        if locale_dir:
            self.locale_dir = Path(locale_dir)
        else:
            self.locale_dir = Path(__file__).parent / "locales"

        self.translations: Dict[str, Dict[str, str]] = {}
        self.supported_locales: List[str] = []

        # Load translations
        self._load_translations()

    def _load_translations(self) -> None:
        """Load translation files from the locale directory."""
        try:
            if not self.locale_dir.exists():
                self.locale_dir.mkdir(parents=True, exist_ok=True)
                self._create_default_translations()

            # Load all JSON translation files
            for locale_file in self.locale_dir.glob("*.json"):
                locale_code = locale_file.stem
                try:
                    with open(locale_file, encoding='utf-8') as f:
                        translations = json.load(f)

                    self.translations[locale_code] = translations
                    self.supported_locales.append(locale_code)

                    self.logger.debug(f"Loaded translations for locale: {locale_code}")

                except Exception as e:
                    self.logger.error(f"Failed to load translations for {locale_code}: {e}")

            if not self.supported_locales:
                self.logger.warning("No translations loaded, creating default English translations")
                self._create_default_translations()

        except Exception as e:
            self.logger.error(f"Failed to initialize translations: {e}")
            # Fallback to minimal English translations
            self.translations["en-US"] = {"error": "Error", "success": "Success"}
            self.supported_locales = ["en-US"]

    def _create_default_translations(self) -> None:
        """Create default translation files for common locales."""
        default_translations = {
            "en-US": {
                # ETL Process Messages
                "etl.extraction.started": "Data extraction started",
                "etl.extraction.completed": "Data extraction completed successfully",
                "etl.extraction.failed": "Data extraction failed",
                "etl.transformation.started": "Data transformation started",
                "etl.transformation.completed": "Data transformation completed successfully",
                "etl.transformation.failed": "Data transformation failed",
                "etl.loading.started": "Data loading started",
                "etl.loading.completed": "Data loading completed successfully",
                "etl.loading.failed": "Data loading failed",

                # Data Quality Messages
                "quality.validation.started": "Data quality validation started",
                "quality.validation.passed": "Data quality validation passed",
                "quality.validation.failed": "Data quality validation failed",
                "quality.score": "Quality Score",
                "quality.issues.found": "Data quality issues found",

                # Error Messages
                "error.connection.failed": "Connection failed",
                "error.authentication.failed": "Authentication failed",
                "error.permission.denied": "Permission denied",
                "error.timeout": "Operation timed out",
                "error.resource.not.found": "Resource not found",
                "error.invalid.configuration": "Invalid configuration",
                "error.data.corrupted": "Data corruption detected",

                # Status Messages
                "status.healthy": "System is healthy",
                "status.degraded": "System performance is degraded",
                "status.unhealthy": "System is unhealthy",
                "status.maintenance": "System is under maintenance",

                # Compliance Messages
                "compliance.gdpr.enabled": "GDPR compliance enabled",
                "compliance.data.retention": "Data retention policy applied",
                "compliance.encryption.enabled": "Data encryption enabled",
                "compliance.audit.logged": "Audit trail logged",

                # Performance Messages
                "performance.slow.query": "Slow query detected",
                "performance.high.memory": "High memory usage detected",
                "performance.optimization.applied": "Performance optimization applied",

                # General Messages
                "success": "Success",
                "error": "Error",
                "warning": "Warning",
                "info": "Information",
                "started": "Started",
                "completed": "Completed",
                "failed": "Failed",
                "processing": "Processing",
                "cancelled": "Cancelled"
            },

            "es-ES": {
                # ETL Process Messages
                "etl.extraction.started": "Extracción de datos iniciada",
                "etl.extraction.completed": "Extracción de datos completada exitosamente",
                "etl.extraction.failed": "Extracción de datos falló",
                "etl.transformation.started": "Transformación de datos iniciada",
                "etl.transformation.completed": "Transformación de datos completada exitosamente",
                "etl.transformation.failed": "Transformación de datos falló",
                "etl.loading.started": "Carga de datos iniciada",
                "etl.loading.completed": "Carga de datos completada exitosamente",
                "etl.loading.failed": "Carga de datos falló",

                # Data Quality Messages
                "quality.validation.started": "Validación de calidad de datos iniciada",
                "quality.validation.passed": "Validación de calidad de datos aprobada",
                "quality.validation.failed": "Validación de calidad de datos falló",
                "quality.score": "Puntuación de Calidad",
                "quality.issues.found": "Problemas de calidad de datos encontrados",

                # Error Messages
                "error.connection.failed": "Conexión falló",
                "error.authentication.failed": "Autenticación falló",
                "error.permission.denied": "Permiso denegado",
                "error.timeout": "Operación expiró",
                "error.resource.not.found": "Recurso no encontrado",
                "error.invalid.configuration": "Configuración inválida",
                "error.data.corrupted": "Corrupción de datos detectada",

                # Status Messages
                "status.healthy": "Sistema está saludable",
                "status.degraded": "Rendimiento del sistema está degradado",
                "status.unhealthy": "Sistema no está saludable",
                "status.maintenance": "Sistema está en mantenimiento",

                # General Messages
                "success": "Éxito",
                "error": "Error",
                "warning": "Advertencia",
                "info": "Información",
                "started": "Iniciado",
                "completed": "Completado",
                "failed": "Falló",
                "processing": "Procesando",
                "cancelled": "Cancelado"
            },

            "fr-FR": {
                # ETL Process Messages
                "etl.extraction.started": "Extraction de données démarrée",
                "etl.extraction.completed": "Extraction de données terminée avec succès",
                "etl.extraction.failed": "Extraction de données échouée",
                "etl.transformation.started": "Transformation de données démarrée",
                "etl.transformation.completed": "Transformation de données terminée avec succès",
                "etl.transformation.failed": "Transformation de données échouée",
                "etl.loading.started": "Chargement de données démarré",
                "etl.loading.completed": "Chargement de données terminé avec succès",
                "etl.loading.failed": "Chargement de données échoué",

                # Data Quality Messages
                "quality.validation.started": "Validation de qualité des données démarrée",
                "quality.validation.passed": "Validation de qualité des données réussie",
                "quality.validation.failed": "Validation de qualité des données échouée",
                "quality.score": "Score de Qualité",
                "quality.issues.found": "Problèmes de qualité des données trouvés",

                # Error Messages
                "error.connection.failed": "Connexion échouée",
                "error.authentication.failed": "Authentification échouée",
                "error.permission.denied": "Permission refusée",
                "error.timeout": "Opération expirée",
                "error.resource.not.found": "Ressource non trouvée",
                "error.invalid.configuration": "Configuration invalide",
                "error.data.corrupted": "Corruption de données détectée",

                # Status Messages
                "status.healthy": "Système en bonne santé",
                "status.degraded": "Performance du système dégradée",
                "status.unhealthy": "Système en mauvaise santé",
                "status.maintenance": "Système en maintenance",

                # General Messages
                "success": "Succès",
                "error": "Erreur",
                "warning": "Avertissement",
                "info": "Information",
                "started": "Démarré",
                "completed": "Terminé",
                "failed": "Échoué",
                "processing": "En cours",
                "cancelled": "Annulé"
            }
        }

        # Create translation files
        for locale, translations in default_translations.items():
            locale_file = self.locale_dir / f"{locale}.json"
            try:
                with open(locale_file, 'w', encoding='utf-8') as f:
                    json.dump(translations, f, indent=2, ensure_ascii=False)

                self.translations[locale] = translations
                if locale not in self.supported_locales:
                    self.supported_locales.append(locale)

                self.logger.info(f"Created default translations for locale: {locale}")

            except Exception as e:
                self.logger.error(f"Failed to create translations file for {locale}: {e}")

    def set_locale(self, locale: str) -> bool:
        """Set the current locale.
        
        Args:
            locale: Locale code to set
            
        Returns:
            True if locale was set successfully, False otherwise
        """
        if locale in self.supported_locales:
            self.current_locale = locale
            self.logger.info(f"Locale set to: {locale}")
            return True
        else:
            self.logger.warning(f"Unsupported locale: {locale}, keeping current: {self.current_locale}")
            return False

    def get_text(self, key: str, locale: Optional[str] = None, **kwargs) -> str:
        """Get localized text for a given key.
        
        Args:
            key: Translation key
            locale: Specific locale to use (defaults to current locale)
            **kwargs: Variables for string formatting
            
        Returns:
            Localized text or the key if translation not found
        """
        target_locale = locale or self.current_locale

        # Try target locale first
        if target_locale in self.translations:
            translation = self.translations[target_locale].get(key)
            if translation:
                try:
                    return translation.format(**kwargs) if kwargs else translation
                except (KeyError, ValueError) as e:
                    self.logger.warning(f"Translation formatting error for key '{key}': {e}")
                    return translation

        # Fallback to default locale
        if target_locale != self.default_locale and self.default_locale in self.translations:
            translation = self.translations[self.default_locale].get(key)
            if translation:
                try:
                    return translation.format(**kwargs) if kwargs else translation
                except (KeyError, ValueError) as e:
                    self.logger.warning(f"Translation formatting error for key '{key}': {e}")
                    return translation

        # Fallback to key itself
        self.logger.debug(f"Translation not found for key: {key}")
        return key

    def get_supported_locales(self) -> List[str]:
        """Get list of supported locales.
        
        Returns:
            List of supported locale codes
        """
        return self.supported_locales.copy()

    def detect_locale_from_env(self) -> str:
        """Detect locale from environment variables.
        
        Returns:
            Detected locale code or default locale
        """
        # Check various environment variables
        for env_var in ['LC_ALL', 'LC_MESSAGES', 'LANG', 'LANGUAGE']:
            locale = os.getenv(env_var)
            if locale:
                # Parse locale format (e.g., 'en_US.UTF-8' -> 'en-US')
                locale_code = locale.split('.')[0].replace('_', '-')
                if locale_code in self.supported_locales:
                    return locale_code

                # Try just the language part (e.g., 'en-US' -> 'en')
                language = locale_code.split('-')[0]
                for supported in self.supported_locales:
                    if supported.startswith(language):
                        return supported

        return self.default_locale

    def add_translation(self, locale: str, key: str, text: str) -> None:
        """Add a translation dynamically.
        
        Args:
            locale: Locale code
            key: Translation key
            text: Translation text
        """
        if locale not in self.translations:
            self.translations[locale] = {}
            if locale not in self.supported_locales:
                self.supported_locales.append(locale)

        self.translations[locale][key] = text
        self.logger.debug(f"Added translation for {locale}.{key}")

    def get_locale_info(self, locale: Optional[str] = None) -> Dict[str, Any]:
        """Get information about a locale.
        
        Args:
            locale: Locale code (defaults to current locale)
            
        Returns:
            Locale information dictionary
        """
        target_locale = locale or self.current_locale

        locale_info = {
            "code": target_locale,
            "is_current": target_locale == self.current_locale,
            "is_default": target_locale == self.default_locale,
            "is_supported": target_locale in self.supported_locales,
            "translation_count": len(self.translations.get(target_locale, {}))
        }

        # Add language and region information
        if '-' in target_locale:
            language, region = target_locale.split('-', 1)
            locale_info.update({
                "language": language,
                "region": region
            })
        else:
            locale_info["language"] = target_locale

        return locale_info


# Global I18n manager instance
_i18n_manager: Optional[I18nManager] = None


def get_i18n_manager() -> I18nManager:
    """Get the global I18n manager instance.
    
    Returns:
        I18nManager instance
    """
    global _i18n_manager

    if _i18n_manager is None:
        # Initialize with environment-based configuration
        default_locale = os.getenv("DEFAULT_LOCALE", "en-US")
        locale_dir = os.getenv("LOCALE_DIR")

        _i18n_manager = I18nManager(
            default_locale=default_locale,
            locale_dir=locale_dir
        )

        # Auto-detect locale from environment
        detected_locale = _i18n_manager.detect_locale_from_env()
        _i18n_manager.set_locale(detected_locale)

    return _i18n_manager


def get_text(key: str, locale: Optional[str] = None, **kwargs) -> str:
    """Convenience function to get localized text.
    
    Args:
        key: Translation key
        locale: Specific locale to use
        **kwargs: Variables for string formatting
        
    Returns:
        Localized text
    """
    return get_i18n_manager().get_text(key, locale, **kwargs)


def set_locale(locale: str) -> bool:
    """Convenience function to set locale.
    
    Args:
        locale: Locale code to set
        
    Returns:
        True if locale was set successfully
    """
    return get_i18n_manager().set_locale(locale)


def get_supported_locales() -> List[str]:
    """Convenience function to get supported locales.
    
    Returns:
        List of supported locale codes
    """
    return get_i18n_manager().get_supported_locales()
