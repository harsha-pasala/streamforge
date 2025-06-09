// State management functions
window.dash_clientside = Object.assign({}, window.dash_clientside, {
    clientside: {
        loadInitialState: function(trigger) {
            console.log('loadInitialState called with trigger:', trigger);

            // Check server state immediately
            console.log('Checking server state on page load...');
            fetch('/api/state')
                .then(response => response.json())
                .then(state => {
                    console.log('Server state:', state);
                    
                    // Update UI based on server state
                    if (state.running) {
                        console.log('Generation is running, updating UI...');
                        // Update UI to reflect running state
                        const button = document.getElementById('control-button');
                        if (button) {
                            button.textContent = 'Stop';
                            button.style.backgroundColor = '#FF3621';
                            console.log('Updated button state');
                        }
                        
                        // Show code section if DLT code exists
                        const codeSection = document.getElementById('dlt-code-section');
                        if (codeSection) {
                            codeSection.style.display = 'block';
                            console.log('Showing code section');
                        }
                        
                        // Show export button if DLT code exists
                        const exportButton = document.getElementById('export-button-container');
                        if (exportButton && state.dlt_code) {
                            exportButton.style.display = 'block';
                            console.log('Showing export button');
                        }

                        // Trigger interval timer to start polling
                        const intervalTimer = document.getElementById('interval-timer');
                        if (intervalTimer) {
                            intervalTimer.disabled = false;
                            console.log('Started interval timer');
                        }
                    } else {
                        console.log('No active generation found');
                        // Reset button state
                        const button = document.getElementById('control-button');
                        if (button) {
                            button.textContent = 'Start';
                            button.style.backgroundColor = '#00A86B';
                            console.log('Reset button state to Start');
                        }
                    }
                })
                .catch(error => console.error('Error loading state:', error));

            // Return values from server state
            return [null, null, ''];
        },

        manageState: function(trigger, button_clicks, language, industry, path, section_style, export_style, button_text, button_style) {
            // Handle initial load
            if (trigger) {
                return null;
            }

            // Handle state save
            if (!button_clicks && !language && !industry && !path) {
                return window.dash_clientside.no_update;
            }

            // Return server state
            const state = {
                running: button_text === 'Stop',
                industry: industry,
                iteration_count: 0,  // Will be updated by Python
                start_time: Date.now(),  // Will be updated by Python
                output_path: path,
                selected_language: language,
                selected_industry: industry,
                path_input: path
            };

            return JSON.stringify(state);
        }
    }
}); 