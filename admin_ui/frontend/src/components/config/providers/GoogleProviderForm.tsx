import React from 'react';

interface GoogleProviderFormProps {
    config: any;
    onChange: (newConfig: any) => void;
}

const GoogleProviderForm: React.FC<GoogleProviderFormProps> = ({ config, onChange }) => {
    const handleChange = (field: string, value: any) => {
        onChange({ ...config, [field]: value });
    };

    const isSTT = config.capabilities?.includes('stt');
    const isLLM = config.capabilities?.includes('llm');
    const isTTS = config.capabilities?.includes('tts');

    return (
        <div className="space-y-6">
            <div className="space-y-4">
                <h4 className="font-semibold text-sm border-b pb-2">Authentication</h4>
                <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                    <div className="space-y-2">
                        <label className="text-sm font-medium">API Key (Environment Variable)</label>
                        <input
                            type="text"
                            className="w-full p-2 rounded border border-input bg-background"
                            value={config.api_key || '${GOOGLE_API_KEY}'}
                            onChange={(e) => handleChange('api_key', e.target.value)}
                            placeholder="${GOOGLE_API_KEY}"
                        />
                    </div>
                    {isSTT && (
                        <div className="space-y-2">
                            <label className="text-sm font-medium">GCP Project ID (for Vertex/STT limits)</label>
                            <input
                                type="text"
                                className="w-full p-2 rounded border border-input bg-background"
                                value={config.project_id || ''}
                                onChange={(e) => handleChange('project_id', e.target.value)}
                                placeholder="my-gcp-project-123"
                            />
                        </div>
                    )}
                </div>
            </div>

            {isLLM && (
                <div className="space-y-4">
                    <h4 className="font-semibold text-sm border-b pb-2">LLM Configuration (Gemini)</h4>
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                        <div className="space-y-2">
                            <label className="text-sm font-medium">LLM Base URL</label>
                            <input
                                type="text"
                                className="w-full p-2 rounded border border-input bg-background"
                                value={config.llm_base_url || 'https://generativelanguage.googleapis.com/v1'}
                                onChange={(e) => handleChange('llm_base_url', e.target.value)}
                            />
                        </div>
                        <div className="space-y-2">
                            <label className="text-sm font-medium">LLM Model</label>
                            <input
                                type="text"
                                className="w-full p-2 rounded border border-input bg-background"
                                value={config.llm_model || 'models/gemini-1.5-pro-latest'}
                                onChange={(e) => handleChange('llm_model', e.target.value)}
                            />
                        </div>
                        <div className="space-y-2">
                            <label className="text-sm font-medium">Temperature</label>
                            <input
                                type="number"
                                step="0.1"
                                className="w-full p-2 rounded border border-input bg-background"
                                value={config.llm_temperature || 0.7}
                                onChange={(e) => handleChange('llm_temperature', parseFloat(e.target.value))}
                            />
                        </div>
                        <div className="space-y-2">
                            <label className="text-sm font-medium">Max Output Tokens</label>
                            <input
                                type="number"
                                className="w-full p-2 rounded border border-input bg-background"
                                value={config.llm_max_output_tokens || 8192}
                                onChange={(e) => handleChange('llm_max_output_tokens', parseInt(e.target.value))}
                            />
                        </div>
                    </div>
                </div>
            )}

            {isSTT && (
                <div className="space-y-4">
                    <h4 className="font-semibold text-sm border-b pb-2">STT Configuration</h4>
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                        <div className="space-y-2">
                            <label className="text-sm font-medium">Language Code</label>
                            <input
                                type="text"
                                className="w-full p-2 rounded border border-input bg-background"
                                value={config.stt_language_code || 'en-US'}
                                onChange={(e) => handleChange('stt_language_code', e.target.value)}
                            />
                        </div>
                        <div className="space-y-2">
                            <label className="text-sm font-medium">Sample Rate (Hz)</label>
                            <input
                                type="number"
                                className="w-full p-2 rounded border border-input bg-background"
                                value={config.input_sample_rate_hz || 8000}
                                onChange={(e) => handleChange('input_sample_rate_hz', parseInt(e.target.value))}
                            />
                        </div>
                    </div>
                </div>
            )}

            {isTTS && (
                <div className="space-y-4">
                    <h4 className="font-semibold text-sm border-b pb-2">TTS Configuration</h4>
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                        <div className="space-y-2">
                            <label className="text-sm font-medium">Voice Name</label>
                            <input
                                type="text"
                                className="w-full p-2 rounded border border-input bg-background"
                                value={config.tts_voice_name || 'en-US-Neural2-C'}
                                onChange={(e) => handleChange('tts_voice_name', e.target.value)}
                            />
                        </div>
                        <div className="space-y-2">
                            <label className="text-sm font-medium">Audio Encoding</label>
                            <select
                                className="w-full p-2 rounded border border-input bg-background"
                                value={config.tts_audio_encoding || 'MULAW'}
                                onChange={(e) => handleChange('tts_audio_encoding', e.target.value)}
                            >
                                <option value="MULAW">μ-law (MULAW)</option>
                                <option value="LINEAR16">Linear16</option>
                            </select>
                        </div>
                    </div>
                </div>
            )}
        </div>
    );
};

export default GoogleProviderForm;
