<?php
/**
 * MIBEL Platform - Configuration Management
 *
 * Reads and writes JSON configuration files for technology classification,
 * exceptions, and parameters.
 */

declare(strict_types=1);

class Config
{
    private static string $dir = '/data/config';

    // =========================================================================
    // Classificacao (Technology Classification)
    // =========================================================================

    /**
     * Get all classification entries
     */
    public static function getClassificacao(): array
    {
        return self::readJson('classificacao.json');
    }

    /**
     * Save classification entries
     */
    public static function saveClassificacao(array $data): void
    {
        self::writeJson('classificacao.json', $data);
    }

    // =========================================================================
    // Excecoes (Unit Exceptions)
    // =========================================================================

    /**
     * Get all exception entries
     */
    public static function getExcecoes(): array
    {
        return self::readJson('excecoes.json');
    }

    /**
     * Save exception entries
     */
    public static function saveExcecoes(array $data): void
    {
        self::writeJson('excecoes.json', $data);
    }

    // =========================================================================
    // Parametros (Scale Parameters / ESCALOES)
    // =========================================================================

    /**
     * Get all parameters (ESCALOES dictionary)
     */
    public static function getParametros(): array
    {
        return self::readJson('parametros.json');
    }

    /**
     * Save parameters
     */
    public static function saveParametros(array $data): void
    {
        self::writeJson('parametros.json', $data);
    }

    // =========================================================================
    // Helper: Get all category+zone combinations from parametros.json
    // =========================================================================

    /**
     * Get all categoria_zona keys present in parametros.json
     *
     * Returns flat array of strings like:
     * ["SOLAR_FOT_ES", "SOLAR_FOT_PT", "EOLICA_ES", ...]
     */
    public static function getCategoriasZona(): array
    {
        $parametros = self::getParametros();
        $categoriasZona = [];

        foreach ($parametros as $regime => $categorias) {
            if (is_array($categorias)) {
                foreach (array_keys($categorias) as $categoriaZona) {
                    $categoriasZona[] = $categoriaZona;
                }
            }
        }

        sort($categoriasZona);
        return array_unique($categoriasZona);
    }

    /**
     * Build mapping from classificacao to possible zona suffixes
     *
     * Rules:
     * - COMERC_EXT, CONTRATO_INT, COMERC_NR_EXT, CONS_DIRECTO_EXT → no suffix needed (keep as-is)
     * - Others → append _ES, _PT, or _EXT based on zone
     */
    public static function buildCategoriaZonaMap(): array
    {
        $classificacao = self::getClassificacao();
        $parametros = self::getParametros();

        // Get all categoria_zona keys from parametros
        $existingKeys = [];
        foreach ($parametros as $regime => $categorias) {
            if (is_array($categorias)) {
                foreach (array_keys($categorias) as $key) {
                    $existingKeys[$key] = true;
                }
            }
        }

        // Categories that don't need zone suffix
        $noSuffixCategories = ['COMERC_EXT', 'CONTRATO_INT', 'COMERC_NR_EXT', 'CONS_DIRECTO_EXT'];

        $map = [];
        foreach ($classificacao as $entry) {
            $categoria = $entry['categoria'];
            $regime = $entry['regime'];

            if (in_array($categoria, $noSuffixCategories)) {
                // No suffix needed
                if (isset($existingKeys[$categoria])) {
                    $map[$categoria] = ['zona' => null, 'regime' => $regime];
                }
            } else {
                // Check for _ES, _PT, _EXT variants
                foreach (['_ES', '_PT', '_EXT'] as $suffix) {
                    $key = $categoria . $suffix;
                    if (isset($existingKeys[$key])) {
                        $map[$key] = [
                            'categoria_base' => $categoria,
                            'zona' => trim($suffix, '_'),
                            'regime' => $regime,
                        ];
                    }
                }
            }
        }

        return $map;
    }

    /**
     * Get parameter for a specific categoria_zona
     */
    public static function getParametro(string $categoriaZona): ?array
    {
        $parametros = self::getParametros();

        foreach ($parametros as $regime => $categorias) {
            if (is_array($categorias) && isset($categorias[$categoriaZona])) {
                return array_merge(
                    ['regime' => $regime],
                    $categorias[$categoriaZona]
                );
            }
        }

        return null;
    }

    /**
     * Update a specific categoria_zona parameter
     */
    public static function updateParametro(string $categoriaZona, array $newValues): bool
    {
        $parametros = self::getParametros();

        foreach ($parametros as $regime => &$categorias) {
            if (is_array($categorias) && isset($categorias[$categoriaZona])) {
                $categorias[$categoriaZona] = array_merge(
                    $categorias[$categoriaZona],
                    $newValues
                );
                self::saveParametros($parametros);
                return true;
            }
        }

        return false;
    }

    // =========================================================================
    // Private Helpers
    // =========================================================================

    /**
     * Read and decode a JSON file
     */
    private static function readJson(string $filename): array
    {
        $path = self::$dir . '/' . $filename;

        if (!file_exists($path)) {
            return [];
        }

        $content = file_get_contents($path);
        if ($content === false) {
            throw new \RuntimeException("Failed to read config file: {$filename}");
        }

        $data = json_decode($content, true);
        if (json_last_error() !== JSON_ERROR_NONE) {
            throw new \RuntimeException("Invalid JSON in config file {$filename}: " . json_last_error_msg());
        }

        return $data;
    }

    /**
     * Encode and write a JSON file
     */
    private static function writeJson(string $filename, array $data): void
    {
        $path = self::$dir . '/' . $filename;

        // Ensure directory exists
        if (!is_dir(self::$dir)) {
            mkdir(self::$dir, 0755, true);
        }

        $json = json_encode($data, JSON_PRETTY_PRINT | JSON_UNESCAPED_UNICODE);
        if ($json === false) {
            throw new \RuntimeException("Failed to encode JSON for {$filename}");
        }

        $result = file_put_contents($path, $json);
        if ($result === false) {
            throw new \RuntimeException("Failed to write config file: {$filename}");
        }
    }

    /**
     * Set custom config directory (for testing)
     */
    public static function setConfigDir(string $dir): void
    {
        self::$dir = $dir;
    }
}
