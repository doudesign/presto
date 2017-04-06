/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.raptorx.backup;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;

import java.util.Map;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;

public class BackupModule
        extends AbstractConfigurationAwareModule
{
    private final Map<String, Module> providers;

    public BackupModule(Map<String, Module> providers)
    {
        this.providers = ImmutableMap.<String, Module>builder()
                .put("file", new FileBackupModule())
                .putAll(providers)
                .build();
    }

    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(BackupConfig.class);

        binder.bind(BackupManager.class).in(Scopes.SINGLETON);

        newOptionalBinder(binder, BackupStore.class);

        String provider = buildConfigObject(BackupConfig.class).getProvider();
        if (provider != null) {
            Module module = providers.get(provider);
            if (module == null) {
                binder.addError("Unknown backup provider: %s", provider);
            }
            else {
                install(module);
            }
        }
    }
}
