<?php
/**
 * MIBEL Platform - Ingestão de Dados (Bid Files)
 *
 * Handles upload, listing, and deletion of monthly ZIP files in /data/bids/.
 * Expected filename pattern: curva_pbc_uof_YYYYMM.zip
 */

declare(strict_types=1);

define('BIDS_DIR', '/data/bids');
define('MAX_UPLOAD_BYTES', 512 * 1024 * 1024); // 512 MB
define('FILENAME_PATTERN', '/^curva_pbc_uof_\d{6}\.zip$/');

// ============================================================================
// List files in /data/bids
// ============================================================================

function index(): void
{
    $dir = BIDS_DIR;

    if (!is_dir($dir)) {
        json_response(['files' => [], 'dir' => $dir]);
    }

    $files = [];
    foreach (new DirectoryIterator($dir) as $entry) {
        if ($entry->isDot() || !$entry->isFile()) {
            continue;
        }
        $name = $entry->getFilename();
        if (!preg_match(FILENAME_PATTERN, $name)) {
            continue;
        }
        $files[] = [
            'name'     => $name,
            'size'     => $entry->getSize(),
            'modified' => date('c', $entry->getMTime()),
        ];
    }

    usort($files, fn($a, $b) => strcmp($a['name'], $b['name']));

    json_response(['files' => $files, 'dir' => $dir]);
}

// ============================================================================
// Upload a ZIP file
// ============================================================================

function store(): void
{
    if (empty($_FILES['file'])) {
        error_response('Nenhum ficheiro recebido. Use multipart/form-data com campo "file".', 400);
    }

    $upload = $_FILES['file'];

    if ($upload['error'] !== UPLOAD_ERR_OK) {
        $msg = match ($upload['error']) {
            UPLOAD_ERR_INI_SIZE, UPLOAD_ERR_FORM_SIZE => 'Ficheiro demasiado grande.',
            UPLOAD_ERR_PARTIAL   => 'Upload incompleto.',
            UPLOAD_ERR_NO_FILE   => 'Nenhum ficheiro enviado.',
            default              => 'Erro de upload (código ' . $upload['error'] . ').',
        };
        error_response($msg, 400);
    }

    $name = basename($upload['name']);

    if (!preg_match(FILENAME_PATTERN, $name)) {
        error_response(
            "Nome de ficheiro inválido: \"{$name}\". " .
            "Formato esperado: curva_pbc_uof_YYYYMM.zip",
            422
        );
    }

    if ($upload['size'] > MAX_UPLOAD_BYTES) {
        error_response('Ficheiro demasiado grande (máximo 512 MB).', 413);
    }

    // Validate it is really a ZIP
    $finfo = new finfo(FILEINFO_MIME_TYPE);
    $mime  = $finfo->file($upload['tmp_name']);
    if (!in_array($mime, ['application/zip', 'application/x-zip-compressed', 'application/octet-stream'], true)) {
        // Try by extension as last resort (some systems return octet-stream for zip)
        if (strtolower(pathinfo($name, PATHINFO_EXTENSION)) !== 'zip') {
            error_response("O ficheiro não parece ser um ZIP válido (MIME: {$mime}).", 422);
        }
    }

    $dir = BIDS_DIR;
    if (!is_dir($dir)) {
        if (!mkdir($dir, 0755, true)) {
            error_response("Não foi possível criar o directório de destino: {$dir}", 500);
        }
    }

    $dest = $dir . '/' . $name;
    $overwrite = file_exists($dest);

    if (!move_uploaded_file($upload['tmp_name'], $dest)) {
        error_response('Falha ao mover o ficheiro para o destino.', 500);
    }

    json_response([
        'success'   => true,
        'name'      => $name,
        'size'      => filesize($dest),
        'overwrite' => $overwrite,
    ], 201);
}

// ============================================================================
// Delete a file
// ============================================================================

function destroy(string $filename): void
{
    $name = basename($filename); // strip any path traversal

    if (!preg_match(FILENAME_PATTERN, $name)) {
        error_response("Nome de ficheiro inválido: \"{$name}\".", 422);
    }

    $path = BIDS_DIR . '/' . $name;

    if (!file_exists($path)) {
        error_response("Ficheiro não encontrado: {$name}", 404);
    }

    if (!unlink($path)) {
        error_response("Não foi possível eliminar o ficheiro: {$name}", 500);
    }

    json_response(['success' => true, 'name' => $name]);
}
