/**
 * @NApiVersion 2.1
 * @NScriptType Restlet
 * @NModuleScope SameAccount
 */
define(['N/file', 'N/task', 'N/runtime'], function(file, task, runtime) {
    
    function post(requestBody) {
        try {
            log.audit('RESTlet Called', 'Starting file upload and import process');
            
            if (!requestBody.filename || !requestBody.content || !requestBody.folderId) {
                return {
                    success: false,
                    error: 'Missing required fields: filename, content, folderId'
                };
            }
            
            log.audit('Creating File', 'Filename: ' + requestBody.filename);
            
            var fileObj = file.create({
                name: requestBody.filename,
                fileType: file.Type.CSV,
                contents: requestBody.content,
                folder: requestBody.folderId,
                isOnline: true
            });
            
            var fileId = fileObj.save();
            log.audit('File Created', 'File ID: ' + fileId);
            
            var importTaskId = null;
            
            if (requestBody.savedImportId) {
                log.audit('Starting Import Task', 'Saved Import ID: ' + requestBody.savedImportId);
                
                try {
                    var csvImportTask = task.create({
                        taskType: task.TaskType.CSV_IMPORT
                    });
                    
                    csvImportTask.mappingId = requestBody.savedImportId;
                    csvImportTask.importFile = file.load({id: fileId});
                    
                    importTaskId = csvImportTask.submit();
                    
                    log.audit('Import Task Submitted', 'Task ID: ' + importTaskId);
                    
                    return {
                        success: true,
                        fileId: fileId,
                        importTaskId: importTaskId,
                        message: 'File uploaded and import task started successfully'
                    };
                    
                } catch (importError) {
                    log.error('Import Task Error', importError.toString());
                    
                    return {
                        success: true,
                        fileId: fileId,
                        importTaskId: null,
                        warning: 'File uploaded but import task failed: ' + importError.message,
                        message: 'File uploaded successfully, but you may need to run the import manually'
                    };
                }
            }
            
            return {
                success: true,
                fileId: fileId,
                message: 'File uploaded successfully'
            };
            
        } catch (e) {
            log.error('RESTlet Error', e.toString());
            
            return {
                success: false,
                error: e.message,
                details: e.toString()
            };
        }
    }
    
    function get(requestParams) {
        return {
            success: true,
            message: 'Tradlinx CSV Import RESTlet is active',
            version: '1.0',
            user: runtime.getCurrentUser().name,
            account: runtime.accountId
        };
    }
    
    return {
        post: post,
        get: get
    };
});